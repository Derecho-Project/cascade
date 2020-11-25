#pragma once
#include <iostream>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

int add_epoll(int epoll_fd, int events, int fd);

bool sock_read(int sock, char* buffer, size_t size);

template <typename T>
bool sock_read(int sock, T& obj) {
    return sock_read(sock, reinterpret_cast<char*>(&obj), sizeof(obj));
};

bool sock_write(int sock, const char* buffer, size_t size);

template <typename T>
bool sock_write(int sock, const T& obj) {
    return sock_write(sock, reinterpret_cast<const char*>(&obj), sizeof(obj));
};