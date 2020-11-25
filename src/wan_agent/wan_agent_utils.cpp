#include <iostream>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <wan_agent/wan_agent_utils.hpp>

int add_epoll(int epoll_fd, int events, int fd) {
    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.events = events;
    ev.data.fd = fd;
    return epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev);
}

bool sock_read(int sock, char* buffer, size_t size) {
    if(sock < 0) {
        fprintf(stderr, "WARNING: Attempted to read from closed socket\n");
        return false;
    }

    size_t total_bytes = 0;
    while(total_bytes < size) {
        ssize_t new_bytes = ::read(sock, buffer + total_bytes, size - total_bytes);
        if(new_bytes > 0) {
            total_bytes += new_bytes;
        } else if(new_bytes == 0 || (new_bytes == -1 && errno != EINTR)) {
            return false;
        }
    }
    return true;
};

bool sock_write(int sock, const char* buffer, size_t size) {
    if(sock < 0) {
        fprintf(stderr, "WARNING: Attempted to write to closed socket\n");
        return false;
    }

    size_t total_bytes = 0;
    while(total_bytes < size) {
        //MSG_NOSIGNAL makes send return a proper error code if the socket has been
        //closed by the remote, rather than crashing the entire program with a SIGPIPE
        ssize_t bytes_written = send(sock, buffer + total_bytes, size - total_bytes, MSG_NOSIGNAL);
        if(bytes_written >= 0) {
            total_bytes += bytes_written;
        } else if(bytes_written == -1 && errno != EINTR) {
            std::cerr << "socket::write: Error in the socket! Errno " << errno << std::endl;
            return false;
        }
    }
    return true;
};
