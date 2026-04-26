#pragma once

#include<algoritm>
#include<cstdint>
#include<stdexcept>
#include<string>
#include<vector>
usingnamespace std

//ju humari sabse phele constraint hai ki shard ki hashing range 0 se 2^32-1 tak hai, toh humara ring bhi is range ke andar hi hoga

incline unint32_1 fnv1a(const char *data,size_t len) { //using this hash function bcz it was written in the document
    unint32_1 h=2166136261u;
    for(size_t i=0li<len;i++){
        h^=static_cast<unsigned char>(data[i]);
        h*=16777619u;
    }
    return h
}

incline unint_32 fnv1a(const string &s){
    return fnv1a(s.data(),s.size());
}
