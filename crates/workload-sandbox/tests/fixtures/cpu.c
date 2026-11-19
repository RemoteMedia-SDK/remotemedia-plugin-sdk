#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <ifaddrs.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/prctl.h>
#include <sys/statvfs.h>
#include <unistd.h>

static int check_isolation(void) {
    if (getuid() != 65532 || getgid() != 65532) return 2;
    if (access("/inputs/source/evidence", R_OK)) return 3;
    if (access("/var/run/docker.sock", F_OK) == 0) return 18;
    if (access("/mnt/d", F_OK) == 0) return 19;
    if (access("/dev/nvidia0", F_OK) == 0) return 20;
    if (access("/sys/fs/cgroup/cgroup.procs", F_OK) == 0) return 21;
    if (open("/inputs/source/write", O_WRONLY | O_CREAT, 0600) >= 0) return 4;
    int file = open("/work/private", O_WRONLY | O_CREAT, 0600);
    if (file < 0) return 5;
    close(file);
    struct statvfs disk;
    if (statvfs("/work", &disk) || disk.f_blocks * disk.f_frsize != 16 * 1024 * 1024) return 6;
    struct ifaddrs *entries;
    if (getifaddrs(&entries)) return 7;
    for (struct ifaddrs *it = entries; it; it = it->ifa_next) {
        if (strcmp(it->ifa_name, "lo")) return 8;
    }
    freeifaddrs(entries);
    FILE *status = fopen("/proc/self/status", "r");
    if (!status) return 9;
    char line[256];
    int caps = 0;
    while (fgets(line, sizeof(line), status)) {
        if (strncmp(line, "CapEff:", 7) == 0) caps = strtoull(line + 7, NULL, 16) == 0;
    }
    fclose(status);
    if (!caps) return 10;
    return prctl(PR_GET_NO_NEW_PRIVS, 0, 0, 0, 0) == 1 ? 0 : 22;
}

static int serve(void) {
    int listener = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr = {.sin_family=AF_INET, .sin_addr.s_addr=htonl(INADDR_LOOPBACK), .sin_port=htons(24566)};
    if (listener < 0 || bind(listener, (struct sockaddr *)&addr, sizeof(addr)) || listen(listener, 1)) return 11;
    for (;;) {
        int peer = accept(listener, NULL, NULL);
        if (peer < 0) return 12;
        char msg[32];
        ssize_t size;
        while ((size = read(peer, msg, sizeof(msg))) > 0) {
            if (write(peer, msg, size) != size) return 13;
        }
        close(peer);
    }
}

static int relay(const char *endpoint) {
    if (strcmp(endpoint, "127.0.0.1:24566")) return 14;
    struct sockaddr_in addr = {.sin_family=AF_INET, .sin_addr.s_addr=htonl(INADDR_LOOPBACK), .sin_port=htons(24566)};
    for (int retry = 0; retry < 20; retry++) {
        int peer = socket(AF_INET, SOCK_STREAM, 0);
        if (peer < 0) return 15;
        if (connect(peer, (struct sockaddr *)&addr, sizeof(addr)) == 0) {
            char msg[32];
            ssize_t size = read(STDIN_FILENO, msg, sizeof(msg));
            if (size <= 0 || write(peer, msg, size) != size) return 16;
            size = read(peer, msg, sizeof(msg));
            if (size <= 0 || write(STDOUT_FILENO, msg, size) != size) return 1;
            return read(peer, msg, sizeof(msg)) != 0;
        }
        close(peer);
        usleep(50000);
    }
    return 17;
}

int main(int argc, char **argv) {
    if (argc != 2) return 1;
    if (!strncmp(argv[1], "127.0.0.1:", 10)) return relay(argv[1]);
    int result = check_isolation();
    if (result) return result;
    if (!strcmp(argv[1], "serve")) return serve();
    if (!strcmp(argv[1], "block")) sleep(30);
    return 0;
}
