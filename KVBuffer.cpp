//
// Created by kxw on 2/17/17.
//

#include "KVBuffer.h"
#include <string>
#include <iostream>
#include <fstream>
#include <cstdlib>
using namespace std;
//////构造函数，主要完成初始化化工作
//kv ** kv_buffer;////这个二维数据主要是用来缓存将源文件分成小文件时写入小文件的kv，很显然每一个子文件需要有一个buffer数组，
//int * cursor;//用来标记每个缓存数组游标走到哪里了
//int EveFileBufferNum;////每个文件缓存多少条kv对儿
//int sub_file_num;
string inttostring_kv_buffer_(int he){
    char t[256];
    string s;

    sprintf(t, "%d", he);
    s = t;
    return s;
}
KVBuffer::KVBuffer(int sub_file_num,int EveFileBufferNum) {
    this->sub_file_num = sub_file_num;
    this->EveFileBufferNum = EveFileBufferNum;
    cursor = new int[sub_file_num];
    kv_buffer = new struct kv * [sub_file_num];
    for(int i =0;i<sub_file_num;i++){
        kv_buffer[i] = new struct kv [EveFileBufferNum];
        cursor[i] = 0;///初始化游标值为0
    }
}


int KVBuffer::insert(string k, string v, int file_no,string basepath,char split_kv ){
    if(file_no<0) {
        cout<<"BUG:kv对所在的文件编号小于0！"<<endl;
        return -1;
    }
    int cursor_of_file_no =cursor[file_no];///获取当前文件buffer的游标
    if(full_or_not(file_no)){//判断当前文件buffer是否已满，是则刷新到文件中，根据指定的参数文件编号和基本路径
        flushToFile(file_no,basepath,split_kv);
        cursor_of_file_no = cursor[file_no];//如果刷新到文件了，重新获取游标
    }
    ////插入当前kv对儿，并且游标增1
    kv_buffer[file_no][cursor_of_file_no].k = k;
    kv_buffer[file_no][cursor_of_file_no].v = v;
    cursor[file_no]++;
    return 0;
}
bool KVBuffer::full_or_not(int file_no) {
    if(file_no>=0){
        int cursor_of_file_no =cursor[file_no];
        if(cursor_of_file_no == EveFileBufferNum) //b比如有5个空间，索引是0-4，当游标等于4时说明空间被填满，则4+1=5！
            return true;
        return false;
    }
   else{
        cout<<"BUG:full_or_not函数中，kv对所在的文件编号小于0！"<<endl;
        exit(0);
    }
}
int KVBuffer::flushToFile(int file_no, string basepath,char split_kv) {
    int cursor_of_file_no =cursor[file_no];
    if(cursor_of_file_no>0) {//说明缓存中有元素！需要刷新到磁盘文件上
        ofstream out;
        out.open((basepath+"/"+inttostring_kv_buffer_(file_no)).c_str(),ios::ate|ios::app);
        if(!out){
            cout<<"BUG：：KVBuffer::flushToFile：输出文件"<<basepath<<"/"<<file_no<<"无法打开！"<<endl;
            exit(0);
        }

        for (int i=0; i <cursor_of_file_no; i++) {
            out <<kv_buffer[file_no][i].k << split_kv << kv_buffer[file_no][i].v<<endl;
        }

        ///都刷新完成了，下面将游标置为初始位置
        out.close();
        cursor[file_no] = 0;

    }
    return 0;
}
int KVBuffer::flushReminderToFile(string basepath, char split_kv) {//虽然缓存区没满，但是要结束源文件转化为小源文件了，因此将缓存区剩余的所有kv都写到磁盘对应文件上去。
    for(int i = 0;i<sub_file_num;i++){
        if(cursor[i]>0){
            flushToFile(i,basepath,split_kv);
        }
    }
    return 0;
}
KVBuffer::~KVBuffer() {
    delete[] cursor;///释放游标数组
    cursor =NULL;
    ////释放缓存数组
    for(int i =0;i<sub_file_num;i++){
       delete [] kv_buffer[i] ;
        kv_buffer[i] = NULL;
    }
    delete [] kv_buffer;
    kv_buffer = NULL;
}
