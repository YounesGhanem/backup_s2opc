// udp_latency_test.c
// OPC UA PubSub latency logger (subscriber with UADP config)

#include <stdio.h>
#include <stdint.h>
#include <inttypes.h>
#include <string.h>
#include <time.h>

#include "sopc_assert.h"
#include "sopc_atomic.h"
#include "sopc_date_time.h"
#include "sopc_logger.h"
#include "sopc_macros.h"
#include "sopc_network_layer.h"
#include "sopc_reader_layer.h"
#include "sopc_sub_sockets_mgr.h"
#include "sopc_threads.h"
#include "sopc_time_reference.h"
#include "sopc_udp_sockets.h"
#include "sopc_helper_endianness_cfg.h"
#include "sopc_dataset_ll_layer.h"

#define MCAST_PORT "4840"
#define MCAST_ADDR "232.1.2.100"
#define LOCAL_INTERFACE "10.51.140.223"
#define LOG_FILE "latency_log.csv"
#define CTX_VALUE (uintptr_t) 1
#define MAX_BUFFER_SIZE 4096

static SOPC_Buffer* buffer = NULL;
static int32_t stop = 0;
static FILE* logFile = NULL;
static SOPC_PubSubConfiguration* configuration = NULL;
static SOPC_PubSubConnection* subConnection = NULL;

static void setup_connection(void)
{
    configuration = SOPC_PubSubConfiguration_Create();
    SOPC_ASSERT(configuration != NULL);

    SOPC_PubSubConfiguration_Allocate_SubConnection_Array(configuration, 1);
    subConnection = SOPC_PubSubConfiguration_Get_SubConnection_At(configuration, 0);

    SOPC_PubSubConnection_Allocate_ReaderGroup_Array(subConnection, 1);
    SOPC_ReaderGroup* group = SOPC_PubSubConnection_Get_ReaderGroup_At(subConnection, 0);

    SOPC_ReaderGroup_Set_SecurityMode(group, SOPC_SecurityMode_None);
    SOPC_ReaderGroup_Allocate_DataSetReader_Array(group, 1);
    SOPC_DataSetReader* reader = SOPC_ReaderGroup_Get_DataSetReader_At(group, 0);

    SOPC_ReaderGroup_Set_GroupId(group, 1245);
    SOPC_ReaderGroup_Set_GroupVersion(group, 963852);
    SOPC_ReaderGroup_Set_PublisherId_UInteger(group, 123);

    SOPC_DataSetReader_Set_DataSetWriterId(reader, 123);
    SOPC_DataSetReader_Allocate_FieldMetaData_Array(reader, SOPC_TargetVariablesDataType, 5);

    SOPC_PubSub_ArrayDimension dim = { .valueRank = -1, .arrayDimensions = NULL };
    SOPC_FieldMetaData* meta = NULL;

    // Timestamp (uint64)
    meta = SOPC_DataSetReader_Get_FieldMetaData_At(reader, 0);
    SOPC_FieldMetaData_ArrayDimension_Move(meta, &dim);
    SOPC_FieldMetaData_Set_BuiltinType(meta, SOPC_UInt64_Id);

    // Byte
    meta = SOPC_DataSetReader_Get_FieldMetaData_At(reader, 1);
    SOPC_FieldMetaData_ArrayDimension_Move(meta, &dim);
    SOPC_FieldMetaData_Set_BuiltinType(meta, SOPC_Byte_Id);

    // UInt16
    meta = SOPC_DataSetReader_Get_FieldMetaData_At(reader, 2);
    SOPC_FieldMetaData_ArrayDimension_Move(meta, &dim);
    SOPC_FieldMetaData_Set_BuiltinType(meta, SOPC_UInt16_Id);

    // DateTime
    meta = SOPC_DataSetReader_Get_FieldMetaData_At(reader, 3);
    SOPC_FieldMetaData_ArrayDimension_Move(meta, &dim);
    SOPC_FieldMetaData_Set_BuiltinType(meta, SOPC_DateTime_Id);

    // UInt32
    meta = SOPC_DataSetReader_Get_FieldMetaData_At(reader, 4);
    SOPC_FieldMetaData_ArrayDimension_Move(meta, &dim);
    SOPC_FieldMetaData_Set_BuiltinType(meta, SOPC_UInt32_Id);
}

static void log_latency(uint64_t sentTime)
{
    // Get actual time since start of the system
    uint64_t now_us = SOPC_TimeReference_GetCurrent();

    // Calculate latency between sending and reception
    int64_t latency_us = (int64_t)(now_us - sentTime);

    // Write in log file: current time, latency in us
    fprintf(logFile, "%" PRIu64 ",%" PRId64 "\n", now_us, latency_us);
    fflush(logFile);

    // Print latency in console as well
    printf("Latency: %" PRId64 " us\n", latency_us);

    // Alert if latency > 6 ms
    if (latency_us > 6000000)
    {
        printf("[WARN] Latency too high: %" PRId64 " us\n", latency_us);
    }
}

static void readyToReceive(void* ctx, SOPC_Socket sock)
{
    SOPC_UNUSED_ARG(ctx);
    if (SOPC_Atomic_Int_Get(&stop)) return;

    SOPC_ReturnStatus status = SOPC_UDP_Socket_ReceiveFrom(sock, buffer);
    if (status == SOPC_STATUS_OK && buffer->length > 1)
    {
        const SOPC_UADP_NetworkMessage_Reader_Configuration readerConf = {
            .pGetSecurity_Func = NULL,
            .callbacks = SOPC_Reader_NetworkMessage_Default_Readers,
            .checkDataSetMessageSN_Func = NULL,
            .updateTimeout_Func = NULL,
            .targetConfig = NULL,
            .targetVariable_Func = NULL
        };

        SOPC_UADP_NetworkMessage* uadp_nm = NULL;
        SOPC_NetworkMessage_Error_Code decodeResult = SOPC_UADP_NetworkMessage_Decode(
            buffer, &readerConf, subConnection, &uadp_nm);

        if (decodeResult == SOPC_NetworkMessage_Error_Code_None && uadp_nm && uadp_nm->nm)
        {
            SOPC_Dataset_LL_DataSetMessage* dsm = SOPC_Dataset_LL_NetworkMessage_Get_DataSetMsg_At(uadp_nm->nm, 0);
            if (dsm)
            {
                const SOPC_Variant* field = SOPC_Dataset_LL_DataSetMsg_Get_Variant_At(dsm, 0);
                if (field && field->BuiltInTypeId == SOPC_UInt64_Id && field->ArrayType == SOPC_VariantArrayType_SingleValue)
                {
                    log_latency(field->Value.Uint64);
                }
            }
            SOPC_UADP_NetworkMessage_Delete(uadp_nm);
        }
    }
    SOPC_Buffer_SetPosition(buffer, 0);
}

static void tick(void* tickCtx)
{
    SOPC_ASSERT(CTX_VALUE == (uintptr_t) tickCtx);
}

int main(void)
{
    printf("Starting udp_latency_test (subscriber)\n");
    SOPC_Socket sock;
    SOPC_Socket_AddressInfo* listenAddr = SOPC_UDP_SocketAddress_Create(false, MCAST_ADDR, MCAST_PORT);
    buffer = SOPC_Buffer_Create(MAX_BUFFER_SIZE);

    logFile = fopen(LOG_FILE, "w");
    if (!logFile)
    {
        perror("Cannot open log file");
        return -1;
    }
    fprintf(logFile, "timestamp,latency_ms\n");

    SOPC_Helper_Endianness_Check();
    setup_connection();

    SOPC_ReturnStatus status = SOPC_UDP_Socket_CreateToReceive(listenAddr, LOCAL_INTERFACE, true, true, &sock);

    if (status == SOPC_STATUS_OK)
    {
        SOPC_Sub_Sockets_Timeout timeout = { .callback = &tick, .pContext = (void*) CTX_VALUE, .period_ms = 1000 };
        SOPC_Sub_SocketsMgr_Initialize(NULL, 0, &sock, 1, readyToReceive, &timeout, 0);
    }

    while (status == SOPC_STATUS_OK && !SOPC_Atomic_Int_Get(&stop))
    {
        SOPC_Sleep(100);
    }

    SOPC_Sub_SocketsMgr_Clear();
    fclose(logFile);
    SOPC_Buffer_Delete(buffer);
    SOPC_UDP_Socket_Close(&sock);
    SOPC_UDP_SocketAddress_Delete(&listenAddr);
    SOPC_PubSubConfiguration_Delete(configuration);

    return 0;
}
