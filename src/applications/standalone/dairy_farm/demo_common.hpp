#pragma once
#include <iostream>
#include <opencv2/opencv.hpp>
#include <vector>
#include <cascade/cascade.hpp>
#include <cascade/object.hpp>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fstream>
#include <string>
#include "config.h"

/**
 * Tensor Flow GPU configuration
 * configure gpu settings
 * This is a serialized version of ConfigOptions, please see Config.proto
 */
/* GPU:1, 100 pct GPU memory, allow growth */
#define INIT_100PCT_GROWTH  0x0a,0x07,0x0a,0x03,0x47,0x50,0x55,0x10,0x01,0x32,0x0b,0x09,0x00,0x00,0x00,0x00,0x00,0x00,0xf0,0x3f,0x20,0x01
/* GPU:1, 90 pct GPU memory, allow growth */
#define INIT_90PCT_GROWTH   0x0a,0x07,0x0a,0x03,0x47,0x50,0x55,0x10,0x01,0x32,0x0b,0x09,0xcd,0xcc,0xcc,0xcc,0xcc,0xcc,0xec,0x3f,0x20,0x01
/* GPU:1, 80 pct GPU memory, allow growth */
#define INIT_80PCT_GROWTH   0x0a,0x07,0x0a,0x03,0x47,0x50,0x55,0x10,0x01,0x32,0x0b,0x09,0x9a,0x99,0x99,0x99,0x99,0x99,0xe9,0x3f,0x20,0x01
/* GPU:1, 70 pct GPU memory, allow growth */
#define INIT_70PCT_GROWTH   0x0a,0x07,0x0a,0x03,0x47,0x50,0x55,0x10,0x01,0x32,0x0b,0x09,0x66,0x66,0x66,0x66,0x66,0x66,0xe6,0x3f,0x20,0x01
/* GPU:1, 60 pct GPU memory, allow growth */
#define INIT_60PCT_GROWTH   0x0a,0x07,0x0a,0x03,0x47,0x50,0x55,0x10,0x01,0x32,0x0b,0x09,0x33,0x33,0x33,0x33,0x33,0x33,0xe3,0x3f,0x20,0x01
/* GPU:1, 50 pct GPU memory, allow growth */
#define INIT_50PCT_GROWTH   0x0a,0x07,0x0a,0x03,0x47,0x50,0x55,0x10,0x01,0x32,0x0b,0x09,0x00,0x00,0x00,0x00,0x00,0x00,0xe0,0x3f,0x20,0x01
/* GPU:1, 40 pct GPU memory, allow growth */
#define INIT_40PCT_GROWTH   0x0a,0x07,0x0a,0x03,0x47,0x50,0x55,0x10,0x01,0x32,0x0b,0x09,0x9a,0x99,0x99,0x99,0x99,0x99,0xd9,0x3f,0x20,0x01
/* GPU:1, 30 pct GPU memory, allow growth */
#define INIT_30PCT_GROWTH   0x0a,0x07,0x0a,0x03,0x47,0x50,0x55,0x10,0x01,0x32,0x0b,0x09,0x33,0x33,0x33,0x33,0x33,0x33,0xd3,0x3f,0x20,0x01
/* GPU:1, 20 pct GPU memory, allow growth */
#define INIT_20PCT_GROWTH   0x0a,0x07,0x0a,0x03,0x47,0x50,0x55,0x10,0x01,0x32,0x0b,0x09,0x9a,0x99,0x99,0x99,0x99,0x99,0xc9,0x3f,0x20,0x01
/* GPU:1, 10 pct GPU memory, allow growth */
#define INIT_10PCT_GROWTH   0x0a,0x07,0x0a,0x03,0x47,0x50,0x55,0x10,0x01,0x32,0x0b,0x09,0x9a,0x99,0x99,0x99,0x99,0x99,0xb9,0x3f,0x20,0x01

#define DEFAULT_TFE_CONFIG  INIT_100PCT_GROWTH

#define PHOTO_HEIGHT    (240)
#define PHOTO_WIDTH     (352)
#define PHOTO_OUTPUT_BUFFER_SIZE    (PHOTO_HEIGHT*PHOTO_WIDTH*3*sizeof(float))

using namespace derecho::cascade;

inline int preprocess_photo(void* in_buf, size_t in_size, void* out_buf, size_t out_size) {
    if (out_size < PHOTO_OUTPUT_BUFFER_SIZE) {
        std::cerr << "Error: preprocess_photo needs an output buffer more than "
                  << PHOTO_OUTPUT_BUFFER_SIZE << " bytes." << std::endl;
        return -1;
    }
    std::vector<unsigned char> decode_buf(in_size);
    std::memcpy(static_cast<void*>(decode_buf.data()),static_cast<const void*>(in_buf),in_size);
    cv::Mat mat = cv::imdecode(decode_buf, cv::IMREAD_COLOR);
    std::vector<float> array;
    cv::resize(mat, mat, cv::Size(272,384));
    for (int c=0; c<3; c++) {           // channels GBR->RGB
        for (int i=0; i<PHOTO_HEIGHT; i++) {     // height
            for (int j=0; j<PHOTO_WIDTH; j++) { // width
                int _i = i+16;
                int _j = j+16;
                array.push_back(static_cast<float>(mat.data[(_i * 272 + _j) * 3 + (2 - c)]) / 255);
            }
        }
    }
    std::memcpy(out_buf,array.data(),PHOTO_OUTPUT_BUFFER_SIZE);
    return 0;
}

typedef struct __attribute__ ((packed)) {
    uint64_t    photo_id;
    uint64_t    timestamp;
    char        data[PHOTO_OUTPUT_BUFFER_SIZE];
} FrameData;

typedef struct __attribute__ ((packed)) {
    uint64_t    photo_id;
    uint64_t    inference_us;
    uint64_t    put_us;
} CloseLoopReport;

inline ObjectWithStringKey get_photo_object(const char* key, const char* photo_file, uint64_t photo_id = 0ul) {
    int fd;
    struct stat st;
    void* file_data;

    // open and map file
    if(stat(photo_file, &st) || access(photo_file, R_OK)) {
        std::cerr << "file " << photo_file << " is not readable." << std::endl;
        return ObjectWithStringKey::IV;
    }

    if((S_IFMT & st.st_mode) != S_IFREG) {
        std::cerr << photo_file << " is not a regular file." << std::endl;
        return ObjectWithStringKey::IV;
    }

    if((fd = open(photo_file, O_RDONLY)) < 0) {
        std::cerr << "Failed to open file(" << photo_file << ") in readonly mode with "
                  << "error:" << strerror(errno) << "." << std::endl;
        return ObjectWithStringKey::IV;
    }

    if((file_data = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE,
                         fd, 0))
       == MAP_FAILED) {
        std::cerr << "Failed to map file(" << photo_file << ") with "
                  << "error:" << strerror(errno) << "." << std::endl;
        return ObjectWithStringKey::IV;
    }

    // load data
    FrameData frame_data;
    frame_data.photo_id = photo_id;
    frame_data.timestamp = get_time();
    preprocess_photo(file_data,st.st_size,reinterpret_cast<void*>(frame_data.data),PHOTO_OUTPUT_BUFFER_SIZE);
    std::string frame_name(key);
    // create Object
    ObjectWithStringKey ret("/dairy_farm/front_end/"+frame_name, reinterpret_cast<const uint8_t*>(&frame_data),sizeof(frame_data));

    // release resources;
    munmap(file_data, st.st_size);
    close(fd);
    usleep(1000);

    return ret;
}

extern void initialize_tf_context();
