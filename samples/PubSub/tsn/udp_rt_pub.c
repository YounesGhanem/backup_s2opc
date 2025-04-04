#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef _WIN32
#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <process.h>
#pragma comment(lib, "ws2_32.lib")
#else
#include <unistd.h>
#include <signal.h>
#include <poll.h>
#include <time.h>
#include <getopt.h>
#endif

#include "sopc_atomic.h"
#include "sopc_assert.h"
#include "sopc_udp_sockets.h"
// #include "sopc_udp_sockets_custom.h"
#include "sopc_helper_endianness_cfg.h"
#include "sopc_network_layer.h"
#include "sopc_date_time.h"
#include "sopc_macros.h"

#define DEFAULT_CYCLE_TIME 1000000
#define DEFAULT_WAKE_DELAY 500000
#define DEFAULT_SO_PRIORITY 3
#define DEFAULT_MCAST_PORT "4840"
#define DEFAULT_MCAST_ADDR "232.1.2.100"
#define DEFAULT_ETH_INTERFACE "enp1s0"
#define ONE_SEC 1000000000

static volatile int stopPublisher = 0;

#ifdef _WIN32
BOOL WINAPI handle_signal(DWORD type)
{
    if (type == CTRL_C_EVENT || type == CTRL_BREAK_EVENT || type == CTRL_CLOSE_EVENT)
    {
        stopPublisher = 1;
        return TRUE;
    }
    return FALSE;
}
#else
static void handle_signal(int sig)
{
    (void)sig;
    stopPublisher = 1;
}
#endif

static void normalize_timespec(struct timespec* ts)
{
    while (ts->tv_nsec >= ONE_SEC)
    {
        ts->tv_sec += 1;
        ts->tv_nsec -= ONE_SEC;
    }
    while (ts->tv_nsec < 0)
    {
        ts->tv_sec -= 1;
        ts->tv_nsec += ONE_SEC;
    }
}

static void usage(const char* progname)
{
    printf("Usage: %s [options]\n", progname);
    printf("  -i [name]          : network interface (default: %s)\n", DEFAULT_ETH_INTERFACE);
    printf("  -c [nanoseconds]   : cycle time (default: %d)\n", DEFAULT_CYCLE_TIME);
    printf("  -d [nanoseconds]   : wake delay (default: %d)\n", DEFAULT_WAKE_DELAY);
    printf("  -p [priority]      : SO_PRIORITY (default: %d)\n", DEFAULT_SO_PRIORITY);
    printf("  -a [address]       : multicast address (default: %s)\n", DEFAULT_MCAST_ADDR);
    printf("  -u [port]          : UDP port (default: %s)\n", DEFAULT_MCAST_PORT);
    printf("  -h                 : print help and exit\n");
}
#ifdef _WIN32
 // Minimal getopt substitute for Windows
 int optind = 1;
 char* optarg = NULL;
 int getopt(int argc, char* const argv[], const char* optstring)
 {
     if (optind >= argc || argv[optind][0] != '-') return -1;
     char opt = argv[optind][1];
     const char* optpos = strchr(optstring, opt);
     if (!optpos) return '?';
     if (*(optpos + 1) == ':' && optind + 1 < argc)
     {
         optarg = argv[++optind];
     }
     else
     {
         optarg = NULL;
     }
     return argv[optind++][1];
 }
 #else
 #include <unistd.h>
 #endif
int main(int argc, char* argv[])
{
    const char* interface = DEFAULT_ETH_INTERFACE;
    const char* mcastAddr = DEFAULT_MCAST_ADDR;
    const char* mcastPort = DEFAULT_MCAST_PORT;
    uint64_t cycle_time = DEFAULT_CYCLE_TIME;
    uint64_t wake_delay = DEFAULT_WAKE_DELAY;
    int so_priority = DEFAULT_SO_PRIORITY;

#ifdef _WIN32
    SetConsoleCtrlHandler(handle_signal, TRUE);
#else
    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);
#endif

    int opt;
    while ((opt = 
#ifdef _WIN32
            getopt(argc, argv, "hi:c:d:p:a:u:")
#else
            getopt(argc, argv, "hi:c:d:p:a:u:")
#endif
        ) != -1)
    {
        switch (opt)
        {
        case 'h':
            usage(argv[0]);
            return 0;
        case 'i': interface = optarg; break;
        case 'c': cycle_time = (uint64_t) atol(optarg); break;
        case 'd': wake_delay = (uint64_t) atol(optarg); break;
        case 'p': so_priority = atoi(optarg); break;
        case 'a': mcastAddr = optarg; break;
        case 'u': mcastPort = optarg; break;
        default:
            usage(argv[0]);
            return 1;
        }
    }

    // TODO: Replace with your SOPC_NetworkMessage creation and socket code
    printf("Simulated publisher config:\n");
    printf("Interface: %s\n", interface);
    printf("Cycle Time: %llu ns\n", (unsigned long long)cycle_time);
    printf("Wake Delay: %llu ns\n", (unsigned long long)wake_delay);
    printf("SO Priority: %d\n", so_priority);
    printf("Multicast Address: %s\n", mcastAddr);
    printf("Multicast Port: %s\n", mcastPort);

    // Dummy sleep to simulate loop
    while (!stopPublisher)
    {
#ifdef _WIN32
        Sleep(100);
#else
        usleep(100000);
#endif
    }

    printf("Publisher stopped.\n");
    return 0;
}
