//
// Created by kxw on 1/15/17.
//

#include "MyHash.h"
#include "murmurhash.h"
#include <fstream>
#include <vector>
MyHash::MyHash(uint64_t bin_num,uint64_t Mem_size_B,uint64_t partition_size_B, uint64_t buffer_size_B){
    gb=new GroupingBuffer(buffer_size_B);/////????如何在一个类里创建另一个类的对象作为成员，并且调用构造函数
    this->bin_num = bin_num;//哈希桶个数
    this->spill_file_num = 0;//当前溢写的文件数据为0
    this->current_file_num =0;//当前处理的溢写文件编号为0
    this->mem_size_B = Mem_size_B;//设定哈希表内存限制
    this->current_size_B = 0;
    this->partition_size_B = partition_size_B;//设定partition分区大小
    this->spill_bin_begin = 0;
    this->spill_bin_cursor = bin_num-1;//溢写游标从最后一个桶开始，溢写完成后向前走，游标值减小
    base = new base_node[bin_num];
}
uint64_t MyHash::get_hash_id(string k,const unsigned int seed) {
    uint64_t murmur_id = MurmurHash64A (k.c_str(), k.size(), seed);
    uint64_t bin_id = murmur_id%bin_num;//查看映射到哪个桶中
    return bin_id;
}
/*
 * 当哈希表存储量达到mem_size_B上限了，就会调用该函数，将hash表中从尾部往前走的大小为partition_size_B的聚合结果溢写到一个文件中
 *
 * */
int MyHash::spill_hash_bin_tofile(string basepath, char split_kv, char split_vv) {
    if(current_size_B == 0) return 0;//如果没有可以写的kv缓存对就直接返回
    if(spill_bin_cursor<0) {
        cout<<"spill_hash_bin_file():BUG无可溢出桶！"<<endl;
        exit(0);
    }
    //cout<<"哈希内存大小满为："<<current_size_B<<"字节"<<endl;
    //cout<<"开始溢写到文件"<<spill_file_num<<endl;
    fstream out;
    out.open((basepath+"/"+inttostring(spill_file_num)).c_str(),ios::out|ios::in);//以二进制形式
    if(!out){
        ofstream hehe;
        hehe.open((basepath+"/"+inttostring(spill_file_num)).c_str());
        hehe.close();
        //cout<<"要写入分组文件："<<basepath+"/"+inttostring(file_id)<<"打不开！"<<endl;
        //exit(0);
        out.open((basepath+"/"+inttostring(spill_file_num)).c_str(),ios::out|ios::in);//以二进制形式
        if(!out){
            cout<<"要溢写文件："<<basepath+"/"+inttostring(spill_file_num)<<"打不开！"<<endl;
            exit(0);
        }
    }
    //cout<<"spill_hashtable 到文件："<<spill_file_num<<" 前"<<endl;
    //print_hashtable();
    uint64_t  data_size=0;
    int bin_beg = spill_bin_cursor;//从bin游标开始，向前
    string id="bin_id";
    for(;bin_beg>=0&&(data_size+base[bin_beg].fk_sum)<partition_size_B;bin_beg--){
        ////输出关键字bin_id\t 哈希桶id,fk_sum 为了方便后续恢复部分聚合结果
        out<<id<<split_kv<<bin_beg<<split_vv<<base[bin_beg].fk_sum<<endl;//形式就是key kv分隔符 value 换行
        //cout<<"base["<<bin_beg<<"].fk_sum ="<<base[bin_beg].fk_sum<<endl;
        if(base[bin_beg].fk_sum >0&&base[bin_beg].next!=NULL) {//这个桶中有可以刷新的缓存kv对
            // cout<<""<<endl;
            list_node *ptr = base[bin_beg].next;//获取第一个要刷新的节点
            while(ptr!=NULL&& base[bin_beg].fk_sum>0){//说明有下一个节点，并且经过统计下一个节点还有剩余kv对被缓存
               //说明当前这个节点有缓存kv
                   // out.seekp(ptr->fk_size-last_offset,ios::cur);
                        out<<ptr->k<< split_kv <<ptr->listofv<<endl;//形式就是key kv分隔符 value 换行
                        out.flush();
                        int kv_size = ptr->k.size()+ptr->listofv.size();
                        base[bin_beg].fk_sum = base[bin_beg].fk_sum-kv_size;//本桶字节量减去key和value list
                     // cout<<base[bin_beg].fk_sum<<endl;
                        data_size +=kv_size;//输出到partition中的数据量增加
                        current_size_B -=kv_size;//当前哈希表内存减少
                        string().swap(ptr->listofv);//将缓存清空！
                ptr = ptr->next;
            }//至此，一个桶写入完成！
            out.flush();
        }
        ///下面这句话主要是为了检验程序是否正确，因此调完程序可以删除
        if(base[bin_beg].fk_sum != 0) {

            cout<<"for循环里，BUG:桶"<<bin_beg<<"中的缓存kv写入文件后，fk_sum还大于0！"<<endl;
            //cout<<"base["<<bin_beg<<"].fk_num="<<base[bin_beg].fk_sum<<endl;
        }
       //下面要释放空间了，释放掉这个桶对应的所有链表
        list_node * current = base[bin_beg].next;
        while(current!=NULL){//当下一个不是空节点时
            list_node * next = current->next;
            delete  current;
            current = next;
        }
        base[bin_beg].fk_sum = spill_file_num+1;//当前哈希桶映射到文件0时，存的是1，映射到文件i时，存的是i+1，是为了区分当fk_sum=0时，
        base[bin_beg].next = NULL;
        // 到底是溢写到了文件0中，还是没有kv对插入呢，当next域为NULL并且fk_sum大于0时，就是溢写到了文件fk_sum中，next域为NULL并且fk_sum=0才是没有kv'
        ////释放空间完成！
    }//至此，文件sub_file涉及的所有桶中的kv对都写到了文件中。
    if(data_size==0&&bin_beg>=0&&base[bin_beg].fk_sum>partition_size_B){//如果第一个桶的大小就超过partition,上面循环肯定跳出了，那么就溢写一个桶到文件中。
        ///输出关键字bin_id\t 哈希桶id,fk_sum 为了方便后续恢复部分聚合结果
        out<<id<<split_kv<<bin_beg<<split_vv<<base[bin_beg].fk_sum<<endl;//形式就是key kv分隔符 value 换行
        if(base[bin_beg].fk_sum >0&&base[bin_beg].next!=NULL) {//这个桶中有可以刷新的缓存kv对
            // cout<<""<<endl;
            list_node *ptr = base[bin_beg].next;//获取第一个要刷新的节点
            while(ptr!=NULL&& base[bin_beg].fk_sum>0){//说明有下一个节点，并且经过统计下一个节点还有剩余kv对被缓存
                //说明当前这个节点有缓存kv
                // out.seekp(ptr->fk_size-last_offset,ios::cur);
                out<<ptr->k<< split_kv <<ptr->listofv<<endl;//形式就是key kv分隔符 value 换行
                out.flush();
                int kv_size = ptr->k.size()+ptr->listofv.size();
                base[bin_beg].fk_sum = base[bin_beg].fk_sum-kv_size;//本桶字节量减去key和value list
                data_size +=kv_size;//输出到partition中的数据量增加
                current_size_B -=kv_size;//当前哈希表内存减少
                string().swap(ptr->listofv);//将缓存清空！
                ptr = ptr->next;
            }//至此，一个桶写入完成！
            out.flush();
        }
        ///下面这句话主要是为了检验程序是否正确，因此调完程序可以删除
        if(base[bin_beg].fk_sum != 0) {
            cout<<"for循环外：BUG:桶"<<bin_beg<<"中的缓存kv写入文件后，fk_sum还大于0！"<<endl;
            //cout<<"base["<<bin_beg<<"].fk_num="<<base[bin_beg].fk_sum<<endl;
        }
        //下面要释放空间了，释放掉这个桶对应的所有链表
        list_node * current = base[bin_beg].next;
        while(current!=NULL){//当下一个不是空节点时
            list_node * next = current->next;
            delete  current;
            current = next;
        }
        base[bin_beg].fk_sum = spill_file_num+1;//当前哈希桶映射到文件0时，存的是1，映射到文件i时，存的是i+1，是为了区分当fk_sum=0时，
        base[bin_beg].next = NULL;
        // 到底是溢写到了文件0中，还是没有kv对插入呢，当next域为NULL并且fk_sum大于0时，就是溢写到了文件fk_sum中，next域为NULL并且fk_sum=0才是没有kv'
        ////释放空间完成！
        bin_beg--;//输出了一个桶，则可溢桶游标减1。
    }
    out<<endl;////加上一个分隔符号，标记下面是后续到来的keyvalue对
    out.close();
    //cout<<"spill_hashtable 到文件："<<spill_file_num<<" 后！"<<endl;
    //print_hashtable();
    if(data_size>0) {
       spill_file_num++;
        spill_bin_cursor = bin_beg;//更新溢写哈希桶游标
    }//也就是说确实溢写了
    //cout<<"哈希内存大小变为："<<current_size_B<<"字节"<<endl;
   // cout<<"溢写哈希桶的尾部游标变为："<<spill_bin_cursor<<endl;

}
bool MyHash::insert_kv_lru(string k, string v,string base_path, char split_kv,char split_vv) {//参数的意思分别为要插入的key、value、子文件路经、kv分隔符
    unsigned int seed = 59;//通常是一个质数，所有hash用同一个种子
    uint64_t bin_id  = get_hash_id(k,seed);//获取桶id
    //cout<<base+bin_id<<endl;
    if(current_size_B+k.size()+v.size()>mem_size_B){//判断插入当前这个kv对之后是否会导致整个内存溢出，大于mem_size_B，则是，触发溢写操作！
        spill_hash_bin_tofile(base_path,split_kv,split_vv);//先溢写部分哈希桶中的kv对到磁盘文件
    }
    if(spill_bin_cursor>=0&&spill_bin_begin>=0&&bin_id<=spill_bin_cursor&&bin_id>=spill_bin_begin ) {//说明映射到的是内存中的哈希桶，有可能spill_bin_cursor等于-1，也就是说最后一个哈希桶也溢写出去了
        if ((base + bin_id)->next == NULL) {//说明是第一个节点
            //  struct list_node  current_strut;    如果想保留退出本函数还存在的变量值，不要在栈上分配空间！，在堆上分配空间
            //struct list_node * current =& current_strut;
            struct list_node *current = new list_node;//结构体数组动态分配
            current->k = k;
            current->listofv = v;
            (base + bin_id)->fk_sum += v.size() + k.size();//获取字符串长度，每个字符占一个字节？
            //(base+bin_id)->fk_sum+=k.size()+1;
            current_size_B+= v.size() + k.size();//插入keyvalue则当前内存缓存计数增加相应字节数
            current->next = NULL;
            (base + bin_id)->next = current;//将第一个节点插入base中
            // cout<<"current = "<<current<<endl;
        } else {
            list_node *pt = base[bin_id].next, *prev = pt;
            while (pt != NULL && pt->k.compare(k) != 0) {//pt的key小于k
                prev = pt;
                pt = pt->next;
            }//跳出循环的话说明走到链表尾部，说明pt的key大于等于当前key
            if (pt != NULL && (pt->k.compare(k) == 0)) {//pt的key等于k
                (base + bin_id)->fk_sum += v.size() + 1;//找到当前key对应的频率统计节点，将频率增加
                current_size_B+= v.size() + 1;//插入keyvalue则当前内存缓存计数增加相应字节数
                ///然后将当前节点从当前位置删除，插入到头节点处
                pt->listofv += split_vv + v;
                if (base[bin_id].next != pt) {//如果当前修改的节点就是头节点，那么不用再插入了
                    prev->next = pt->next;
                    pt->next = base[bin_id].next;
                    base[bin_id].next = pt;
                }
            } else {//pt为空，说明当前key节点还没建立
                struct list_node *current = new list_node;
                current->k = k;
                current->listofv = v;
                (base + bin_id)->fk_sum += v.size() + k.size();//获取字符串长度，每个字符占一个字节？
                current_size_B+= v.size() + k.size();//插入keyvalue则当前内存缓存计数增加相应字节数
                ////最近使用的插入头节点
                current->next = base[bin_id].next;
                base[bin_id].next = current;
                ///说明是第一次遇见key，则文件里也需要为key预留空间，则当前桶的总预留空间加上一个key的大小再加上1
                // (base+bin_id)->fk_sum+=(k.size()+1);
            }
        }//////
    }else{//说明当前到来的kv对映射到磁盘上的溢写文件中
        if(base[bin_id].fk_sum>0&&base[bin_id].next==NULL) {
            int file_no = base[bin_id].fk_sum-1;
            string file_no_string = inttostring(file_no);

            if(gb->contain(file_no_string)){//包含则找到key包含的value在后面追加上新的value
                if(gb->full_or_not(v.size()+1)){//包含，则说明只需要追加分隔符value即可，再加入就满了，则先把buffer中的写到文件里去
                    flushtofile(base_path);
                    string k_v = k+"\t"+v;
                    gb->insert(file_no_string,k_v);//插入缓存;
                    //cout<<"插入后"<<endl;
                   // gb->print_gb();
                }//这样有一个问题，就是，当刚才插入的里面有6，但是被刷新到文件了，现在插入的只更新是不够的，
                else {
                    string k_v = k+"\t"+v;
                     gb->update_value(file_no_string,k_v);
                   // cout<<"插入后"<<endl;
                   // gb->print_gb();
                }
            }
            else{//不包含则直接插入key和value
                if(gb->full_or_not(k.size()+v.size())){//再加入就满了，则先把buffer中的写到文件里去,不包含，就直接插入kv，没有v的分隔符呢
                    flushtofile(base_path);
                }
                string k_v = k+"\t"+v;
                 gb->insert(file_no_string,k_v);//插入缓存;
               // cout<<"插入后"<<endl;
               // gb->print_gb();
            }
        } else{
            cout<<"[BUG:]fk_num指定的溢写文件id不大于0！"<<endl;
            exit(0);
        }

    }
    //////////下面是为了调试所用，输出当前kv映射到的那个桶的链表////////////////
   /* cout<<"base["<<bin_id<<"]";
    list_node * pt =  base[bin_id].next;
    while(pt!=NULL){//pt的key小于k
        cout<<"->"<<pt->k<<","<<pt->fk_size;
        pt=pt->next;
    }
    cout<<" sum = "<<base[bin_id].fk_sum<<endl;*/
    //////////下面是为了调试所用，输出当前kv映射到的那个桶的链表////////////////
    ///追加写入kv到对应文件
    /*ofstream out;
    out.open((base_path+"/"+inttostring(file_id)).c_str(),ios::app|ios::ate);
    if(!out){
        cout<<"文件"<<base_path<<"/"<<file_id<<"打开失败！"<<endl;
        exit(0);
    }
    out<<k<<split_kv<<v<<endl;//将kv对写入sub文件
    out.close();*/
    ///追加写入kv到对应文件
    return true;
}
int MyHash::flushtofile(string basepath) {//当要处理文件sub_file
    //cout<<"flushtofile begin://///////////"<<endl;
   // gb->print_gb();
    if((*gb->buffer).empty()) return 0;
    map<string,vector<string> >::iterator it;
    it=(*gb->buffer).begin();
    string k =it->first;
    vector<string> v = it->second;/////////v是所有的value，下一步需要查找hash频率表，找到文件中偏移
    string file_id = k;
    ofstream out;
    out.open((basepath+"/"+file_id).c_str(),ios::ate|ios::app);//以二进制形式
    if(!out){
        ofstream hehe;
        hehe.open((basepath+"/"+file_id).c_str());
        hehe.close();
        //cout<<"要写入分组文件："<<basepath+"/"+inttostring(file_id)<<"打不开！"<<endl;
        //exit(0);
        out.open((basepath+"/"+file_id).c_str(),ios::ate|ios::app);//以二进制形式
        if(!out){
            cout<<"要写入分组文件111："<<basepath+"/"+file_id<<"打不开！"<<endl;
            exit(0);
        }
    }

    for(;it!=(*gb->buffer).end();){
        for(int i=0;i<v.size();i++) {
            out << v[i]<<endl;//输出k\tv\nk\tv\n......
        }
        out.flush();
        it++;
        if(it==(*gb->buffer).end()) break;
        k =it->first;
        v = it->second;/////////v是所有的value，下一步需要查找hash频率表，找到文件中偏移量
        if(k.compare(file_id)!=0){
            file_id = k;//不是上次那个文件id，则更新file_id,并且关闭上一个文件，打开新的文件
            out.close();
            out.open((basepath+"/"+file_id).c_str(),ios::ate|ios::app);//以二进制形式
            if(!out){
                ofstream hehe;
                hehe.open((basepath+"/"+file_id).c_str());
                hehe.close();
                //cout<<"要写入分组文件："<<basepath+"/"+inttostring(file_id)<<"打不开！"<<endl;
                //exit(0);
                out.open((basepath+"/"+file_id).c_str(),ios::ate|ios::app);//以二进制形式
                if(!out){
                    cout<<"要写入分组文件2222："<<basepath+"/"+file_id<<"打不开！"<<endl;
                    exit(0);
                }
            }

        }

    }
    out.close();//关闭最后一个打开的文件
    (*gb->buffer).clear();////释放map的所有内部节点
    gb->current_size_B=0;//清空buffer的同时，将计数器置为0
    //cout<<"flushtofile Result://///////////"<<endl;
   // gb->print_gb();
   // cout<<"输出是否是flushfile出错，导致spill到的桶又回来了？？？？？？"<<endl;
   // print_hashtable();
    return 0;
}
/*清空当前哈希表，输出kv对到outpath文件，begin和end是要输出的哈希桶范围bin_id起始位置
 *如果范围正确，返回0，删除哈希桶中链表节点，否则返回-1，清空越界哈希表中桶，则失败！
 * */
int MyHash::clearHashtable(string outpath, char split_kv, char split_vv) {//这个vv应该是，
    int begin = spill_bin_begin;
    int end = spill_bin_cursor;
    if(begin>=0&&end>=0&&begin<bin_num&&end<bin_num&&begin<=end){//当起始位置和终止位置都在哈希桶范围内
        ofstream out;
        out.open((outpath).c_str(),ios::app|ios::ate);//以二进制形式
        if(!out){
            ofstream hehe;
            hehe.open((outpath).c_str());
            hehe.close();
            //cout<<"要写入分组文件："<<basepath+"/"+inttostring(file_id)<<"打不开！"<<endl;
            //exit(0);
            out.open(outpath.c_str(),ios::app|ios::ate);//以二进制形式
            if(!out){
                cout<<"要写入分组文件111："<<outpath<<"打不开！"<<endl;
                exit(0);
            }
        }
        for(int i=begin;i<=end;i++){
            if(base[i].fk_sum >0) {//这个桶中有可以刷新的缓存kv对
                // cout<<""<<endl;
              //base[i].fk_sum;////预计出memsize删除掉当前桶中的缓存键值对后，剩下的空间。
                list_node *ptr = base[i].next;//获取第一个要刷新的节点
                while(ptr!=NULL&& base[i].fk_sum>0){//说明有下一个节点，并且经过统计下一个节点还有剩余kv对被缓存
                    if(!ptr->listofv.empty()){//说明当前这个节点有缓存kv
                        vector<string> split_v;
                        splitByMyChar_returnVectorWithoutDimension(split_v,ptr->listofv,',');
                        base[i].fk_sum = base[i].fk_sum-ptr->k.size();//字节数据减去key的大小
                        current_size_B -=ptr->k.size();
                        for(int j=0;j<split_v.size();j++){
                            out <<ptr->k<< split_kv << split_v[j] <<split_vv;//形式就是key kv分隔符 value 换行
                            out.flush();
                            base[i].fk_sum = base[i].fk_sum-split_v[j].size()- 1;//减去value和value分隔符
                            current_size_B = current_size_B-split_v[j].size()- 1;
                        }
                        base[i].fk_sum = base[i].fk_sum+1;//在循环里面多减掉了一个分割符号
                        current_size_B +=1;
                        string().swap(ptr->listofv);//将缓存清空！
                    }
                    ptr = ptr->next;
                }//至此，一个桶写入完成！
                out.flush();

            }
            ///下面这句话主要是为了检验程序是否正确，因此调完程序可以删除
            if(base[i].fk_sum != 0){
              //  cout<<"clearHashtable():"<<endl;
              //  cout<<"BUG:桶"<<i<<"中的缓存kv写入文件后，fk_sum还大于0！,fk_sum ="<<base[i].fk_sum<<endl;
            }
            //下面要释放空间了，释放掉这个桶对应的所有链表
            list_node * current = base[i].next;
            while(current!=NULL){//当下一个不是空节点时
                list_node * next = current->next;
                delete  current;
                current = next;
            }
            base[i].next =NULL;
        }//至此，文件sub_file涉及的所有桶中的kv对都写到了文件中。
        out.close();
        return 0;
    }
    return -1;
}
int MyHash::reloadHashtable(string basepath, int file_num,char split_kv, char split_vv) {//split_kv='\t' split_vv = ','
    ifstream in;//但是其实这并不合理对不？可能上一个文件缓存的kv对只有一点，下一个文件和它一起占用buffer_size，这时就要多个文件一起写入缓存，这里还没改写
    in.open((basepath+"/"+inttostring(file_num)).c_str(),ios::in);
    if(!in){
        cout<<basepath+"/"+inttostring(file_num)<<"打不开！"<<endl;
        exit(0);
    }
    string line;
    getline(in,line);//读取第一行，bin_id \t 50000,fk_sum
    int index = line.find_first_of("bin_id",0);//查找包含bin_id关键字
    int bin_id ;
    int fk_sum;
    if (index != string::npos){
        line = line.substr(index+7);//line现在保存bin_id,fk_sum
        index =  line.find_first_of(split_vv,0);
        bin_id = atoi(line.substr(0,index).c_str());
        fk_sum = atoi(line.substr(index+1).c_str());

        base[bin_id].fk_sum = fk_sum;//回填这个桶中已经聚合的数据量
        spill_bin_cursor = bin_id;//本次加载哈希表后，后续可能还有溢写过程，标记溢写的起始位置
    }else{
        cout<<"[BUG:]reload partition 第一行不是id"<<endl;
        exit(0);
    }

    while(getline(in, line)&&line.compare("")!=0){//循环读取聚合结果那一部分888888888888
        list_node * pre =NULL;
        while(line.substr(0,6).compare("bin_id")!=0) {//还有该桶中的键值对构成的链表节点
            index = line.find_first_of(split_kv, 0);//key \t value
            string k = line.substr(0, index);
            string v = line.substr(index + 1);
            struct list_node * current = new list_node;//结构体数组动态分配
            current->k = k;
            current->listofv = v;
            current->next = NULL;
            current_size_B+=k.size()+v.size();//内存占用也随着增大
            if (pre == NULL) {
                (base + bin_id)->next = current;//将第一个节点插入base中
                pre = current;
            } else {
                pre->next = current;
                pre = current;//这里出现了一个大bug，忘记让pre往下走了，导致丢掉节点！！！！！！
            }
            //cout<<"插入一个节点： ["<<current->k<<","<<current->listofv<<"]"<<endl;
           // print_hashtable();
            if (!getline(in, line)||line.empty()) break;//读取下一行的key \t value list
        }//while(line.substr(0,6).compare("bin_id")!=0)
        //当读取的那行包含bin_id则跳出上面循环，下面处理桶id'这一行
        if(line.empty()) break;
        /*这句是针对
        bin_id	1,49
        9	0.142857,0.2,0.142857,0.25,0
        5	0.25,0.142857,0.2,0

        8	0.25
        9	0.25
        9	0.333333*///从不包含bin_id的链表节点行结束之后，因为读到了空行，则从处理链表节点的循环跳出，说明下面没有bin_id了，该开始后续到来的kv对了
        if(!in.eof()) {//跳出循环不是因为读取到文件末尾了
            index = line.find_first_of("bin_id", 0);
            if (index != string::npos) {
                line = line.substr(index + 7);//line现在保存bin_id,fk_sum
                index = line.find_first_of(split_vv, 0);
                bin_id = atoi(line.substr(0, index).c_str());
                fk_sum = atoi(line.substr(index + 1).c_str());
                base[bin_id].fk_sum = fk_sum;//回填这个桶中已经聚合的数据量
            }else{
                cout<<"[BUG:]reload partition 第一行不是id"<<endl;
                exit(0);
            }

        }
    }//循环读取聚合结果那一部分88888888888888888888888888888888888888888888888888888888888
    //cout<<"reload hashtable from file "<<file_num<<endl;
   // print_hashtable();
    spill_bin_begin =bin_id;//下次可以溢出的最尾部的游标是从文件读出的最后一个桶id
    getline(in,line);//由于读到空行出来了，下面处理后续到来得kv对
    while(!in.eof()) {//下面该处理keyvalue对了，看插入哪一个哈希桶
        index = line.find_first_of(split_kv, 0);//key \t value
        string k = line.substr(0, index);
        string v = line.substr(index + 1);
        insert_kv_lru(k, v, basepath, split_kv, split_vv);//插入内存哈希表，这其中也会完成当内存满时溢写新的文件，并且spill_file_num增加
       // cout<<"插入一个节点： ["<<k<<","<<v<<"]"<<endl;
      //  print_hashtable();
        getline(in,line);
    }
    in.close();
}
/*
//在这里，将内存中哈希表键值对写到分组结果文件中并且循环读取partition文件中部分聚合文件和后续映射到的键值对，进行聚合
*/

int MyHash::insert(string k, string v,char split_vv) {//将value插入到hash表的listofv部分，缓存下来！

    unsigned int seed = 59;//通常是一个质数，所有hash用同一个种子
    uint64_t bin_id  = get_hash_id(k,seed);
    //cout<<base+bin_id<<endl;

    if((base+bin_id)->next==NULL){//说明是第一个节点
        //  struct list_node  current_strut;    如果想保留退出本函数还存在的变量值，不要在栈上分配空间！，在堆上分配空间
        //struct list_node * current =& current_strut;
       cout<<"<"<<k<<","<<v<<">:插入listofv失败！"<<endl;
        // cout<<"current = "<<current<<endl;
    }else{
        list_node * pt =  base[bin_id].next,*prev = pt;
        while(pt!=NULL&&pt->k.compare(k)!=0){//pt的key小于k
            prev = pt;
            pt=pt->next;
        }//跳出循环的话说明走到链表尾部，说明pt的key大于等于当前key
        if(pt!=NULL&&(pt->k.compare(k)==0)){//pt的key等于k

            ///判断listofv 是否为null，是则listofv = v,不是 listofv += ,v
            if(pt->listofv.empty()){
                pt->listofv= v;
                current_size_B+=v.size();
                base[bin_id].fk_sum+=v.size();
            } else{//不是第一个value， listofv += ,v
                pt->listofv +=split_vv+v;
                current_size_B+=v.size()+1;
                base[bin_id].fk_sum+=v.size()+1;
            }

        }
        else {//pt为空，说明当前key节点还没建立
            cout<<"<"<<k<<","<<v<<">:插入listofv失败！"<<endl;
        }
    }//// if((base+bin_id)->next==NULL){//说明是第一个节点
    return 0;
}

/*
 * 下面的key_grouping完成主要功能就是：清空一直驻留在内存中的哈希表，生成第一部分分组文件，
 * 然后循环加载partition文件，恢复哈希表，插入kv对，生成分组文件
 * */
int MyHash::key_grouping(char split_kv, char split_vv, string base_path, string outpath) {
    flushtofile(base_path);//将剩余的kv插入到对应的文件中去
   // delete  gb;//kv对缓存已经用完了，释放空间删除掉！后面可能会再用到，不能删除
    //第一步，先清空驻留在内存中的哈希表，里面有对应桶的完整键值对记录，写出到分组文件outpath中
  //  print_hashtable();
    clearHashtable(outpath,split_kv,split_vv);
   // cout<<" clearHashtable后："<<endl;
  //  print_hashtable();
    //第二步，循环加载partion文件，然后聚合输出到outpath，这其中还可能存在加载后的再溢写，就会增加spill_file_num，循环下去
        int i=current_file_num;
        for(;i<spill_file_num;i++){//重复加载，然后清除
          reloadHashtable(base_path,i,split_kv,',');
            flushtofile(base_path);//可能还剩下kv对，需要写到对应文件上
            clearHashtable(outpath,split_kv,split_vv);//将内存聚合好的写到结果文件中
        }
    //clearHashtable(outpath,split_kv,split_vv);//将内存聚合好的写到结果文件中
}
void MyHash::delete_hash_linkedlist() {
    for(int i=0;i<bin_num;i++){//遍历每一个桶
        list_node * current = base[i].next;
        while(current!=NULL){//当下一个不是空节点时
            list_node * next = current->next;
            delete  current;
            current = next;
        }
    }
}
int MyHash::print_hashtable() {
    cout<<"----------------------------------------------------------------"<<endl;
    int begin=0;
    int end = bin_num;
    for(int i=begin;i<end;i++){
            // cout<<""<<endl;
           cout<<"["<<i<<","<<base[i].fk_sum<<"] ->";////预计出memsize删除掉当前桶中的缓存键值对后，剩下的空间。
            list_node *ptr = base[i].next;//获取第一个要刷新的节点
            while(ptr!=NULL&& base[i].fk_sum>0){//说明有下一个节点，并且经过统计下一个节点还有剩余kv对被缓存
                if(!ptr->listofv.empty()){//说明当前这个节点有缓存kv
                   cout<<" <"<<ptr->k<<","<<ptr->listofv<<"> ->";
                }
                ptr = ptr->next;
            }//至此，一个桶写入完成！
        cout<<endl;
        ///下面这句话主要是为了检验程序是否正确，因此调完程序可以删除
    }//至此，文件sub_file涉及的所有桶中的kv对都写到了文件中。
    cout<<"----------------------------------------------------------------"<<endl;
}
MyHash::~MyHash() {
    ////循环释放链表及数组
    delete_hash_linkedlist();
    delete gb;//?????在key_grouping函数中已经用过了，不用再删除
    delete[] base;
}
