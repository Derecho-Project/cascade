#pragma once
#include <iostream>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

int add_epoll(int epoll_fd, int events, int fd);

bool sock_read(int sock, char *buffer, size_t size);

template <typename T>
bool sock_read(int sock, T &obj);

bool sock_write(int sock, const char *buffer, size_t size);

template <typename T>
bool sock_write(int sock, const T &obj);