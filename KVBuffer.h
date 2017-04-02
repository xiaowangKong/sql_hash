//
// Created by kxw on 2/17/17.
//

#ifndef HASH_GROUPING_KVBUFFER_H
#define HASH_GROUPING_KVBUFFER_H

#include <string>

using namespace std;
struct  kv{
    string k;
    string v;
};
class KVBuffer {
public:
    kv ** kv_buffer;////这个二维数据主要是用来缓存将源文件分成小文件时写入小文件的kv，很显然每一个子文件需要有一个buffer数组，
    int * cursor;//用来标记每个缓存数组游标走到哪里了
    int EveFileBufferNum;////每个文件缓存多少条kv对儿
    int sub_file_num;
    KVBuffer(int  sub_file_num,int EveFileBufferNum);
    int insert(string k,string v,int file_no,string basepath, char split_kv);//插入kv以及所属的文件编号
    bool full_or_not(int file_no);//判断是否有文件满，有则刷新到磁盘文件中
    int flushToFile(int file_no,string basepath,char split_kv);
    int flushReminderToFile(string basepath,char split_kv);
   ~KVBuffer();
};


#endif //HASH_GROUPING_KVBUFFER_H
