//
// Created by kxw on 1/17/17.
//

#ifndef HASH_GROUPING_GROUPINGBUFFER_H
#define HASH_GROUPING_GROUPINGBUFFER_H

#include <map>
#include <vector>
#include <string>
#include <cstdlib>
#include <stdint-gcc.h>
using namespace std;
class GroupingBuffer {
public:
    map<string,vector<string> > * buffer;//第一个是存溢写文件编号，下面的是key，value列表，中间以指定的分割符号分割
    uint64_t  buffer_size_B;
    uint64_t  current_size_B;
    GroupingBuffer(uint64_t buffer_size_B);
    bool full_or_not(uint64_t needToInsertKV_inB);
    bool contain(string key);
    int update_value(string key,string value);
    int insert(string k,string v);
    int print_gb();
    ~GroupingBuffer();
};


#endif //HASH_GROUPING_GROUPINGBUFFER_H
