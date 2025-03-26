// udp_multicast_listener.c
#define _WINSOCK_DEPRECATED_NO_WARNINGS
#include <stdio.h>
#include <winsock2.h>
#include <ws2tcpip.h>

#pragma comment(lib, "ws2_32.lib")

#define MCAST_ADDR "232.1.2.100"
#define MCAST_PORT 4840
#define LOCAL_INTERFACE "10.51.140.223"
#define BUFFER_SIZE 4096

int main(void)
{
    WSADATA wsaData;
    SOCKET sock;
    struct sockaddr_in localAddr;
    struct ip_mreq mreq;
    char buffer[BUFFER_SIZE];
    int ret;

    WSAStartup(MAKEWORD(2, 2), &wsaData);

    sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock == INVALID_SOCKET)
    {
        perror("socket");
        return 1;
    }

    // Bind to local address/port
    memset(&localAddr, 0, sizeof(localAddr));
    localAddr.sin_family = AF_INET;
    localAddr.sin_port = htons(MCAST_PORT);
    localAddr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(sock, (struct sockaddr*)&localAddr, sizeof(localAddr)) < 0)
    {
        perror("bind");
        return 1;
    }

    // Join multicast group on specified interface
    mreq.imr_multiaddr.s_addr = inet_addr(MCAST_ADDR);
    mreq.imr_interface.s_addr = inet_addr(LOCAL_INTERFACE);

    if (setsockopt(sock, IPPROTO_IP, IP_ADD_MEMBERSHIP, (char*)&mreq, sizeof(mreq)) < 0)
    {
        perror("setsockopt IP_ADD_MEMBERSHIP");
        return 1;
    }

    printf("Listening for multicast on %s:%d\n", MCAST_ADDR, MCAST_PORT);

    while (1)
    {
        ret = recv(sock, buffer, BUFFER_SIZE, 0);
        if (ret < 0)
        {
            perror("recv");
            break;
        }

        printf("Received %d bytes: ", ret);
        for (int i = 0; i < ret; ++i)
        {
            printf("%02X ", (unsigned char)buffer[i]);
        }
        printf("\n");
    }

    closesocket(sock);
    WSACleanup();

    return 0;
}
