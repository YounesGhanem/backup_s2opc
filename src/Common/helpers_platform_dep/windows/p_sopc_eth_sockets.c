#include <winsock2.h>
#include <windows.h>
#include "sopc_common_constants.h"
#include "sopc_eth_sockets.h"
#include "sopc_mem_alloc.h"
#include "sopc_macros.h"
#include "p_sopc_sockets.h"

struct SOPC_ETH_Socket_ReceiveAddressInfo
{
    //struct sockaddr_ll addr;
    bool recvMulticast;
    bool recvForDest;
    unsigned char recvDestAddr[6];
    bool recvFromSource;
    unsigned char recvSourceAddr[6];
};

SOPC_ReturnStatus SOPC_ETH_Socket_CreateSendAddressInfo(const char* interfaceName,
                                                        const char* destMACaddr,
                                                        SOPC_ETH_Socket_SendAddressInfo** sendAddInfo)
{
    (void) interfaceName;
    (void) destMACaddr;
    (void) sendAddInfo;

    // Ethernet raw socket not supported on Windows
    return SOPC_STATUS_NOT_SUPPORTED;
}


SOPC_ReturnStatus SOPC_ETH_Socket_CreateToSend(SOPC_ETH_Socket_SendAddressInfo* sendAddrInfo,
                                               bool setNonBlocking,
                                               SOPC_Socket* sock)
{
    (void) sendAddrInfo;
    (void) setNonBlocking;
    (void) sock;

    // Not supported in windows
    return SOPC_STATUS_NOT_SUPPORTED;
}




SOPC_ReturnStatus SOPC_ETH_Socket_SendTo(SOPC_Socket sock,
                                         const SOPC_ETH_Socket_SendAddressInfo* sendAddrInfo,
                                         uint16_t etherType,
                                         SOPC_Buffer* buffer)
{
    (void) sock;
    (void) sendAddrInfo;
    (void) etherType;
    (void) buffer;

    // Non supporté sur Windows (AF_PACKET, RAW Ethernet)
    return SOPC_STATUS_NOT_SUPPORTED;
}



void SOPC_ETH_Socket_Close(SOPC_Socket* sock)
{
    if (NULL != sock && SOPC_INVALID_SOCKET != *sock)
    {
        int res = 0;
        S2OPC_TEMP_FAILURE_RETRY(res, closesocket((*sock)->sock));
        SOPC_Free(*sock);
        *sock = SOPC_INVALID_SOCKET;
    }
}



SOPC_ReturnStatus SOPC_ETH_Socket_CreateReceiveAddressInfo(const char* interfaceName,
    bool recvMulticast,
    const char* destMACaddr,
    const char* sourceMACaddr,
    SOPC_ETH_Socket_ReceiveAddressInfo** recvAddInfo)
{
(void) interfaceName;
(void) recvMulticast;
(void) destMACaddr;
(void) sourceMACaddr;

if (NULL == recvAddInfo)
{
return SOPC_STATUS_INVALID_PARAMETERS;
}

// Allocation vide pour satisfaire l'API, mais ne fait rien.
*recvAddInfo = SOPC_Calloc(1, sizeof(SOPC_ETH_Socket_ReceiveAddressInfo));
if (NULL == *recvAddInfo)
{
return SOPC_STATUS_OUT_OF_MEMORY;
}

// Marque le stub comme "non fonctionnel" si jamais appelé.
return SOPC_STATUS_OK;
}



SOPC_ReturnStatus SOPC_ETH_Socket_CreateToReceive(SOPC_ETH_Socket_ReceiveAddressInfo* receiveAddrInfo,
    bool setNonBlocking,
    SOPC_Socket* sock)
{
(void) receiveAddrInfo;
(void) setNonBlocking;

if (NULL == sock)
{
return SOPC_STATUS_INVALID_PARAMETERS;
}

// On retourne une socket invalide, ce mode n'est pas supporté sous Windows
*sock = SOPC_INVALID_SOCKET;

return SOPC_STATUS_NOT_SUPPORTED;
}




SOPC_ReturnStatus SOPC_ETH_Socket_ReceiveFrom(SOPC_Socket sock,
    const SOPC_ETH_Socket_ReceiveAddressInfo* receiveAddrInfo,
    bool checkEtherType,
    uint16_t etherType,
    SOPC_Buffer* buffer)
{
(void) sock;
(void) receiveAddrInfo;
(void) checkEtherType;
(void) etherType;
(void) buffer;

// Ce mode RAW Ethernet n'est pas supporté sous Windows
return SOPC_STATUS_NOT_SUPPORTED;
}


