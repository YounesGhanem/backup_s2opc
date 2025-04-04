/*
 * Licensed to Systerel under one or more contributor license
 * agreements. See the NOTICE file distributed with this work
 * for additional information regarding copyright ownership.
 * Systerel licenses this file to you under the Apache
 * License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/* 
 * NOTE: This sample has been adapted to be compatible with Windows systems.
 * Modified by ext-ghanemyo
 */

 #include <stdbool.h>
 #include <stddef.h>
 #include <stdio.h>
 #include <stdlib.h>
 #include <string.h>
//  #include "sopc_pubsub_config.h"         // Pour SOPC_PubSubConfiguration
//#include "sopc_pubsource_variable.h"    // Pour SOPC_PubSourceVariableConfig
#include "sopc_pub_scheduler.h"         // Pour SOPC_PubScheduler_*
#include "cache.h"
#include "sopc_mem_alloc.h"
#include "sopc_mutexes.h"
#include "sopc_sub_scheduler.h"
#include "sopc_log_manager.h"
#include "sopc_common.h"
#include "sopc_xml_loader.h"
#include "sopc_threads.h"
#include "pubsub.h"
#include <windows.h>
#include <process.h>

 
SOPC_Mutex globalValue_lock;

#define SLEEP_TIMEOUT_MS 20

#define NB_FIELDS_PER_DSM 7

static int32_t gDsmValues[NB_FIELDS_PER_DSM] = {1, 2, 3, 4, 5, 6, 7};

volatile int stopSignal = 0;
 
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
 
 #include "pubsub.h"
 #include "sopc_assert.h"
 
 #define KIND_BENCHMARK_DEFAULT PUBLISHER_BENCH
 #define MAX_BUFFER_SIZE 1024
 
 typedef enum benchmark_kind_t
 {
     PUBLISHER_BENCH,
     SUBSCRIBER_BENCH
 } benchmark_kind_t;
 
 benchmark_kind_t benchKind = KIND_BENCHMARK_DEFAULT;
 int priority = 99;
 char benchKindBuff[64] = {0};


 #ifdef _WIN32
 #include <windows.h>
 BOOL WINAPI signal_stop_program(DWORD signal)
 {
     if (signal == CTRL_C_EVENT || signal == CTRL_BREAK_EVENT || signal == CTRL_CLOSE_EVENT)
     {
         
         printf("CTRL+C or Close signal received. Stopping program...\n");
         stopSignal  = 1;
         return TRUE;
     }
     return FALSE;
 }
 #else
 void signal_stop_program(int signal)
 {
     if (signal == SIGINT || signal == SIGTERM)
     {
         printf("Signal %d received. Stopping program...\n", signal);
         // Libération éventuelle des ressources
     }
 }
 #endif
 
 bool PubSub_common_init(void)
 {
 #ifdef _WIN32
     // Enregistre la fonction de rappel pour les événements console (CTRL+C, fermeture)
     SetConsoleCtrlHandler(signal_stop_program, TRUE);
 #else
     signal(SIGINT, signal_stop_program);
     signal(SIGTERM, signal_stop_program);
 #endif
 
     SOPC_Mutex_Initialization(&globalValue_lock);
 
     // Configuration du log
     const char* logPath = "./logs/";
     SOPC_Log_Configuration logConfiguration = SOPC_Common_GetDefaultLogConfiguration();
     logConfiguration.logLevel = SOPC_LOG_LEVEL_ERROR;
     logConfiguration.logSysConfig.fileSystemLogConfig.logDirPath = logPath;
 
     bool res = (SOPC_STATUS_OK == SOPC_Common_Initialize(logConfiguration));
 
     if (!res)
     {
         printf("ERROR: Toolkit Initialisation failed\n");
     }
 
     return res;
 }

void PubSub_common_clear(void)
{
    SOPC_Common_Clear();
    SOPC_Mutex_Clear(&globalValue_lock);
}

static int setConfig_fromFile(SOPC_PubSubConfiguration** config, const char* configPath)
{
    if (NULL == config || NULL == configPath)
    {
        printf("Invalid parameters setConfig_fromFile\n");
        return false;
    }

    bool res = true;

    FILE* fd = fopen(configPath, "r");
    if (NULL == fd)
    {
        res = false;
        printf("ERROR: Failed to open file %s\n", configPath);
    }
    if (res && NULL != fd)
    {
        *config = SOPC_PubSubConfig_ParseXML(fd);
        if (NULL == *config)
        {
            res = false;
            printf("Failed to parse XML configuration file %s\n", configPath);
        }

        int closed = fclose(fd);
        if (0 != closed)
        {
            printf("ERROR: Failed to close file %s with status %d\n", configPath, closed);
            res = false;
        }
    }
    if (res)
    {
        printf("Publisher configured with file %s\n", configPath);
    }
    return res;
}

// int setConfig_fromFile(SOPC_PubSubConfiguration** config, const char* configPath)
// {
//     if (NULL == config || NULL == configPath)
//     {
//         printf("Invalid parameters setConfig_fromFile\n");
//         return false;
//     }

//     bool res = true;

//     FILE* fd = fopen(configPath, "r");
//     if (NULL == fd)
//     {
//         res = false;
//         printf("ERROR: Failed to open file %s\n", configPath);
//     }
//     if (res && NULL != fd)
//     {
//         *config = SOPC_PubSubConfig_ParseXML(fd);
//         if (NULL == *config)
//         {
//             res = false;
//             printf("Failed to parse XML configuration file %s\n", configPath);
//         }
//         int closed = fclose(fd);
//         if (0 != closed)
//         {
//             printf("ERROR: Failed to close file %s with status %d\n", configPath, closed);
//             res = false;
//         }
//     }
//     if (res)
//     {
//         printf("Publisher configured with file %s\n", configPath);
//     }
//     return res;
// }
SOPC_DataValue* get_source_value(const OpcUa_ReadValueId* nodesToRead, const int32_t nbValues)
{
    SOPC_ASSERT(NULL != nodesToRead && nbValues > 0);
    SOPC_ASSERT(nbValues <= NB_FIELDS_PER_DSM);

    SOPC_DataValue* dvs = SOPC_Calloc((size_t) nbValues, sizeof(SOPC_DataValue));
    SOPC_ASSERT(dvs != NULL);

    for (int32_t i = 0; i < nbValues; ++i)
    {
        SOPC_DataValue* dv = &dvs[i];

        SOPC_DataValue_Initialize(dv);
        SOPC_Variant* var = &dv->Value;
        var->BuiltInTypeId = SOPC_Int32_Id;
        var->ArrayType = SOPC_VariantArrayType_SingleValue;

        SOPC_Mutex_Lock(&globalValue_lock);
        var->Value.Int32 = gDsmValues[i];
        SOPC_Mutex_Unlock(&globalValue_lock);
    }

    return dvs;
}

static void update_value(void)
{
    SOPC_Mutex_Lock(&globalValue_lock);
    for (int i = 0; i < NB_FIELDS_PER_DSM; i++)
    {
        gDsmValues[i] += i + 1;
    }
    SOPC_Mutex_Unlock(&globalValue_lock);
}


bool PubSub_publisher_bench(int priority)
{
    if (priority < 0 || priority > 99)
    {
        return false;
    }
    bool res = true;
    SOPC_ReturnStatus status = SOPC_STATUS_OK;
    SOPC_PubSubConfiguration* config = NULL;
    SOPC_PubSourceVariableConfig* sourceConfig = NULL;

    res = setConfig_fromFile(&config, CONFIG_PUBLISHER_PATH);

    if (res)
    {
        sourceConfig = SOPC_PubSourceVariableConfig_Create(&get_source_value);
        if (sourceConfig == NULL)
        {
            printf("ERROR: publisher callback configuration hasn't been set\n");
            status = SOPC_STATUS_NOK;
        }
        if (SOPC_STATUS_OK == status)
        {
            res = SOPC_PubScheduler_Start(config, sourceConfig, priority);
            if (!res)
            {
                printf("ERROR: publisher scheduler failed starting. Check logs\n");
            }
            else
            {
                printf("Publisher started \n");
            }
        }
        /* Wait for a signal and do your work */
        while (SOPC_STATUS_OK == status && res && 0 == stopSignal)
        {
            SOPC_Sleep(SLEEP_TIMEOUT_MS);
            update_value();
        }
    }
    if (stopSignal)
    {
        printf("Program stop by signal handling\n");
    }
    else
    {
        printf("Program stop caused by previous error. Check logs\n");
    }
    SOPC_PubScheduler_Stop();
    SOPC_PubSubConfiguration_Delete(config);
    SOPC_PubSourceVariableConfig_Delete(sourceConfig);
    return res;
}
 
 static char* enum_bench_type_to_char(benchmark_kind_t var)
 {
     int res = 0;
     if (var == PUBLISHER_BENCH)
     {
         res = sprintf(benchKindBuff, "PUBLISHER_BENCH");
         SOPC_ASSERT(res == strlen("PUBLISHER_BENCH"));
         return benchKindBuff;
     }
     else if (var == SUBSCRIBER_BENCH)
     {
         res = sprintf(benchKindBuff, "SUBSCRIBER_BENCH");
         SOPC_ASSERT(res == strlen("SUBSCRIBER_BENCH"));
         return benchKindBuff;
     }
     else
     {
         res = sprintf(benchKindBuff, "UNKNOWN");
         SOPC_ASSERT(res == strlen("UNKNOWN"));
         return benchKindBuff;
     }
 }

static HANDLE hPublisherThread = NULL;
static DWORD WINAPI publisher_thread_func(LPVOID lpParam)
{
    int priority = *(int*)lpParam;
    int corePublisher = 1;

    //DWORD_PTR mask = (1 << corePublisher);
    DWORD_PTR mask = (1ULL << corePublisher);
    SetThreadAffinityMask(GetCurrentThread(), mask);

    printf("Publisher process attached to core %d\n", corePublisher);
    PubSub_publisher_bench(priority);

    return 0;
}


static bool subscriber_bench(int priority)
{
    if (priority < 0 || priority > 99)
    {
        return false;
    }

    bool res = true;
    SOPC_ReturnStatus status = SOPC_STATUS_OK;
    SOPC_PubSubConfiguration* config = NULL;
    SOPC_SubTargetVariableConfig* targetConfig = NULL;

    res = setConfig_fromFile(&config, CONFIG_SUBSCRIBER_PATH);

    if (res)
    {
        targetConfig = SOPC_SubTargetVariableConfig_Create(&Cache_SetTargetVariables);
        if (targetConfig == NULL)
        {
            printf("ERROR: subscriber callback configuration hasn't been set\n");
            status = SOPC_STATUS_NOK;
        }

        if (SOPC_STATUS_OK == status)
        {
            res = Cache_Initialize(config);
            if (!res)
            {
                printf("ERROR: initializing cache failed\n");
                status = SOPC_STATUS_NOK;
            }
        }

        if (SOPC_STATUS_OK == status)
        {
            res = SOPC_SubScheduler_Start(config, targetConfig, NULL, NULL, NULL, priority);
            if (!res)
            {
                printf("ERROR: subscriber scheduler failed starting. Check logs\n");
            }
            else
            {
                printf("Subscriber started \n");
            }
        }

        // Boucle principale
        while (SOPC_STATUS_OK == status && res && 0 == stopSignal)
        {
#ifdef _WIN32
            Sleep(SLEEP_TIMEOUT_MS); // Windows
#else
            SOPC_Sleep(SLEEP_TIMEOUT_MS); // POSIX/portable
#endif
        }
    }

    if (stopSignal)
    {
        printf("Program stopped by signal\n");
    }
    else
    {
        printf("Program stopped due to error. Check logs\n");
    }

    SOPC_SubScheduler_Stop();
    SOPC_PubSubConfiguration_Delete(config);
    SOPC_SubTargetVariableConfig_Delete(targetConfig);
    Cache_Clear();

    return res;
}

bool PubSub_subscriber_bench(int priority)
{
    bool res = true;

    // Lancer le "publisher" dans un thread séparé
    hPublisherThread = CreateThread(
        NULL,               // default security
        0,                  // default stack size
        publisher_thread_func,  // thread function
        &priority,          // thread param
        0,                  // default creation flags
        NULL                // thread ID
    );

    if (hPublisherThread == NULL)
    {
        printf("Failed to create publisher thread\n");
        return false;
    }

    // Attach subscriber to core 0
    int coreSubscriber = 0;
    DWORD_PTR mask = (1ULL << coreSubscriber);
    if (SetThreadAffinityMask(GetCurrentThread(), mask) == 0)
    {
        printf("Failed to attach subscriber process to core %d\n", coreSubscriber);
        res = false;
    }
    else
    {
        printf("Subscriber process attached to core %d\n", coreSubscriber);
        res = subscriber_bench(priority);
    }

    // Attendre que le thread publisher termine (si nécessaire)
    WaitForSingleObject(hPublisherThread, INFINITE);
    CloseHandle(hPublisherThread);

    return res;
}
 
 static bool benchmark(void)
 {
     bool res = PubSub_common_init();
     if (res)
     {
         if (PUBLISHER_BENCH == benchKind)
         {
             res = PubSub_publisher_bench(priority);
         }
         else if (SUBSCRIBER_BENCH == benchKind)
         {
             res = PubSub_subscriber_bench(priority);
         }
         else
         {
             printf("ERROR: Unknown kind of bench. Check help of program\n");
             res = false;
         }
     }
     PubSub_common_clear();
     return res;
 }
 
 static int check_priority(int prio)
 {
 #ifdef _WIN32
     return 0;
 #else
     if (prio < 0 || prio > 99)
     {
         printf("value %d is not a priority accepted\n", prio);
         printf("Possible values are between 0 and 99 inclusive\n");
         return -1;
     }
     else if (prio > 0)
     {
         uid_t uid = getuid();
         if (0 != uid)
         {
             printf("When thread priority is above 0, run with sudo privileges\n");
             return -2;
         }
     }
     return 0;
 #endif
 }
 
 static void usage(char* fct_name)
 {
     printf("%s [options] \n", fct_name);
     printf("\t-k [pub|sub] : Kind of benchmark (default: %s)\n", enum_bench_type_to_char(KIND_BENCHMARK_DEFAULT));
     printf("\t-p [priority] : Set thread priority (default: %d)\n", priority);
     printf("\t-h            : Show help\n");
 }
 
 int main(int argc, char* argv[])
 {
     char* progname = strrchr(argv[0], '/');
     progname = progname ? 1 + progname : argv[0];
     int opt;
     char buffer[MAX_BUFFER_SIZE] = {0};
 
     while ((opt = getopt(argc, argv, "hk:p:")) != -1)
     {
         switch (opt)
         {
         case 'h':
             usage(progname);
             return 0;
         case 'k':
             strncpy(buffer, optarg, MAX_BUFFER_SIZE - 1);
             break;
         case 'p':
             priority = atoi(optarg);
             break;
         case '?':
         default:
             usage(progname);
             return -1;
         }
     }
 
     if (strlen(buffer) > 0)
     {
         if (strncmp(buffer, "pub", 3) == 0)
         {
             benchKind = PUBLISHER_BENCH;
         }
         else if (strncmp(buffer, "sub", 3) == 0)
         {
             benchKind = SUBSCRIBER_BENCH;
         }
         else
         {
             printf("Invalid -k option value: %s\n", buffer);
             printf("Expected: pub or sub\n");
             return -1;
         }
     }
 
     int res = check_priority(priority);
     if (res != 0)
     {
         return res;
     }
 
     return benchmark() ? 0 : -1;
 }
 