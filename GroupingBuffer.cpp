//
// Created by kxw on 1/17/17.
//

#include "GroupingBuffer.h"
#include <iostream>
using namespace std;
GroupingBuffer::GroupingBuffer(uint64_t buffer_size_B) {
    buffer = new std::map<string,vector<string> >();
    this->buffer_size_B = buffer_size_B;
    this->current_size_B = 0;
}
bool GroupingBuffer::full_or_not(uint64_t needToInsertKV_inB) {///满了为true
     if(current_size_B+needToInsertKV_inB>buffer_size_B) return true;
     else return false;
}
bool GroupingBuffer::contain(string key ) {///包含为true
    map<string,vector<string> >::iterator it;
    it=buffer->find(key);
    if(it != buffer->end())
        return true;
    return false;
}
int GroupingBuffer::update_value(string key, string value) {//后面的以\n来区分
    map<string,vector<string> >::iterator it;
    it=buffer->find(key);
    if(it != buffer->end()){//说明找到
        it->second.push_back(value);//追加value
        current_size_B+=value.size();
        return 0;
    }
    else return -1;//说明没找到
}
int GroupingBuffer::insert(string k, string v) {
    std::vector<string> res;
    res.push_back(v);
    buffer->insert(std::pair<string,vector<string> >(k,res));
    current_size_B+=k.size()+v.size();
    return 0;
}
int GroupingBuffer::print_gb() {
    map<string,vector<string> >::iterator it=buffer->begin();
    while(it!=buffer->end()){
        cout<<it->first<<"  "<<endl;
        for(int i=0;i<it->second.size();i++){
            cout<<it->second[i]<<endl;
        }
        it++;
    }
}
GroupingBuffer::~GroupingBuffer() {
    delete buffer;
}