//
// Created by kxw on 1/15/17.
//

#ifndef HASH_GROUPING_MYHASH_H
#define HASH_GROUPING_MYHASH_H


#include <stdint-gcc.h>
#include <string>
#include <cstdlib>
#include "GroupingBuffer.h"
using namespace std;
struct list_node{//sql hash表，存聚合的kv对，不需要偏移量，分别是k值，value构成的list，还有next
    string k;
    string listofv;//将第二层缓存放入hash表，初始时为null
   list_node* next;
    list_node(){
        next = NULL;
    }
};
struct base_node{//sql hash表，存聚合的kv对，，fk_sum用来溢写出时作为统计信息
    uint64_t fk_sum;
    list_node* next;
    base_node(){
        fk_sum=0;
        next=NULL;
    }
};

class MyHash {
public:
    base_node* base;//用来存hash桶的基数组
    uint64_t bin_num;//一共分多少个桶
    uint64_t spill_file_num;//溢写出的文件个数
    uint64_t current_file_num;//当前处理到的溢写文件编号
    GroupingBuffer* gb;////各个溢写文件的后续映射kv对缓存大小 //uint64_t current_size_B;//当前缓存已占用大小
    uint64_t mem_size_B;//内存可用于装哈希表的大小
    uint64_t current_size_B;
    uint64_t partition_size_B;//内存可用于装哈希表的大小
    int spill_bin_begin;
    int spill_bin_cursor;
    MyHash(uint64_t bin_num, uint64_t Mem_size_B,uint64_t partition_size_B,uint64_t buffer_size_B);//初始化，猪妖完成base数组开辟空间
    uint64_t get_hash_id(string k,const unsigned int seed);
    bool insert_kv_lru(string k,string v,const string base_path, char split_kv,char split_vv);//将读取的kv对插入到内存哈希表
    int spill_hash_bin_tofile(string basepath,char split_kv,char split_vv);
    void delete_hash_linkedlist();//遍历频率统计表，释放所有链表节点的空间
    int flushtofile(string basepath);//传递一个判别每个key所属的文件的函数
    int clearHashtable(string outpath,char split_kv,char split_vv);
    int reloadHashtable(string basepath,int file_num,char split_kv, char split_vv);
    int  key_grouping(char split_kv,char split_vv,string basepath,string outpath);///扫描小源文件，查hash表根据位置将数据插入文件，其中还要构造一个buffer累计各个key，多了再进行插入
    int print_hashtable();
    ///下面是key_grouping的Groupingbuffer相关函数
    int insert(string k,string v,char split_vv);
    ~MyHash();
};


#endif //HASH_GROUPING_MYHASH_H
