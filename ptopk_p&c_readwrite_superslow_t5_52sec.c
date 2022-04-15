#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <dirent.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <time.h>
#include <sys/stat.h>
#include <assert.h>
#include <stdbool.h>
/*
current status:
multithread in single file reading -> performance dropped greatly
multithread on multiple file reading -> perfrormance dropped
multithread heapify (divide in to theadnum works) -> implementing
producer-consumer model -> multithread, 一個thread 讀file (唔做parse)(producer), 另外幾個就負責consume(parse and count) -> failed?
memory map multithread file reading -> DOING!!!!!
*/
#define howManyBuffer 104857
#define bufferSize 40
struct pair time_with_count;  // pair<int,int> in c++
long start_time = 1645491600; // 2022年2月22日Tuesday 01:00:00
long end_time = 1679046032;
int ReadCount = 0;
char producerBuffer[howManyBuffer][bufferSize + 1]; // Confusing part
int count = 0;

typedef struct threadargumentProduceFile produceFileThread;
typedef struct threadargumentConsumeFile consumeFileThread;

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t empty = PTHREAD_COND_INITIALIZER;
pthread_cond_t full = PTHREAD_COND_INITIALIZER;

int usePtr = 0;
int readPtr = 0;

struct pair
{
    int hours;
    int count;
};

struct threadargumentProduceFile
{
    char *dir;
};

struct threadargumentConsumeFile
{
    struct pair *timeWithCountArr;
    int threadNo;
};

void heapify(struct pair *a, int size, int node_idx)
{
    int left_idx = node_idx * 2 + 1;
    int right_idx = node_idx * 2 + 2;

    int check = node_idx; // start with middle (not the deepest node), cuz deepest node always
    // satisfy heap

    if (left_idx < size && a[check].count < a[left_idx].count)
    { // left child bigger than current(also index < size)
        check = left_idx;
    }

    if (right_idx < size && a[check].count < a[right_idx].count)
    { // right child bigger than current
        check = right_idx;
    }

    if (left_idx < size && a[check].count == a[left_idx].count && a[check].hours < a[left_idx].hours)
    { // same count
        check = left_idx;
    }

    if (right_idx < size && a[check].count == a[right_idx].count && a[check].hours < a[right_idx].hours)
    { // same count
        check = right_idx;
    }

    if (check != node_idx)
    { // if heap is changed
        struct pair temp = a[node_idx];
        a[node_idx] = a[check];
        a[check] = temp; // swapping

        heapify(a, size, check); // recursion (chance of "check" not satisfying heap)
    }
}

void TopK(struct pair *a, int k, int n)
{

    for (int i = (n / 2) - 1; i >= 0; i--)
    {
        heapify(a, n, i);
    }

    for (int i = n - 1; i > (n - k - 1); i--)
    {
        struct pair temp = a[0];
        a[0] = a[i];
        a[i] = temp;

        heapify(a, i, 0);
    }
}

void *producer(void *arg)
{
    produceFileThread *producerArg = (produceFileThread *)arg;

    DIR *FD; // directory pointer
    struct dirent *in_file;
    FILE *common_file; // read file in folder
    char target_file[255];

    if (NULL == (FD = opendir(producerArg->dir)))
    {
        fprintf(stderr, "Error : Failed to open input directory - %s\n", strerror(errno));
        fclose(common_file);
        exit(1);
    }

    while ((in_file = readdir(FD)) != NULL)
    {
        if (!strcmp(in_file->d_name, "."))
            continue;
        if (!strcmp(in_file->d_name, ".."))
            continue;
        strcpy(target_file, producerArg->dir);
        strcat(target_file, in_file->d_name);
        // printf("file name: %s\n", target_file);

        common_file = fopen(target_file, "r");

        if (common_file == NULL)
        {
            fprintf(stderr, "Error : Failed to open entry file - %s\n", strerror(errno));
            fclose(common_file);
            exit(1);
        }
        // printf("checkpoint 1\n");
        char tempBuf[41];
        while (fgets(tempBuf, 41, common_file) != NULL) // Confusing part
        {
            pthread_mutex_lock(&mutex); // lock
            // printf("use Ptr:%d\n",usePtr);
            while ((usePtr+1) % howManyBuffer == readPtr)
            {
                    //printf("useptr=%d \treadptr=%d\n", usePtr, readPtr);

                pthread_cond_wait(&empty, &mutex); // wait until buffer 2d array not empty
            }
            // CS
            strcpy(producerBuffer[usePtr], tempBuf);
            usePtr = (usePtr + 1) % howManyBuffer;

            pthread_cond_signal(&full);   // become full slot, signal to consumer
            pthread_mutex_unlock(&mutex); // unlock
        }
        fclose(common_file);
    }

    //FINISH READING ALL FILES
    pthread_mutex_lock(&mutex); // lock
    while ((usePtr+1) % howManyBuffer == readPtr)
    {
        pthread_cond_wait(&empty, &mutex); // wait until buffer 2d array not empty
    }
    // CS
    strcpy(producerBuffer[usePtr], "end");
    usePtr = (usePtr + 1) % howManyBuffer;
    pthread_cond_signal(&full);   // become full slot, signal to consumer
    pthread_mutex_unlock(&mutex); // unlock
}

void *consumer(void *arg)
{
    consumeFileThread *consumerArg = (consumeFileThread *)arg;
    // printf("checkpoint consumer start\n");

    while (1)
    {

        pthread_mutex_lock(&mutex);

        while ((readPtr) == usePtr)
        {
            // printf("nothing in the buffer. waiting...\n");
            pthread_cond_wait(&full, &mutex);
        }

        // printf("got it: %scount: %d\nusePtr: %d\nthread No: %d\n\n", producerBuffer[readPtr], count, readPtr, consumerArg->threadNo);
        char *tempEnd;
        tempEnd = producerBuffer[readPtr];
        // CS
        if (strcmp(tempEnd, "end") == 0)
        {
            //printf("useptr=%d \treadptr=%d\n", usePtr, readPtr);
            pthread_cond_signal(&full);
            pthread_mutex_unlock(&mutex);
            // printf("end consume..\n");
            break;
        }
        else
        {
            readPtr = (readPtr + 1) % howManyBuffer;
            pthread_cond_signal(&empty);
            pthread_mutex_unlock(&mutex);

            char *temp;

            long time_stamp = strtol(tempEnd, &temp, 10); // Confusing part

            int transformedHour = (time_stamp - start_time) / 3600;

            consumerArg->timeWithCountArr[transformedHour].hours = transformedHour;
            consumerArg->timeWithCountArr[transformedHour].count += 1;

        }
    }
    //printf("consumer finished, thread no: %d\n", consumerArg->threadNo);
}

int main(int argc, char **argv) //**argv = console command, e.g (./file.c 1 2)
{
    clock_t start = clock();
    int number_of_hours = ((end_time / 3600 * 3600) - start_time) / 3600 + 1;
    struct pair *timeWithCountArr = calloc(number_of_hours, sizeof(struct pair)); // dynamic array
    int threadNo = 2;
    pthread_t produce, consume[threadNo];
    produceFileThread producerArgument;
    consumeFileThread consumerArgument[threadNo];

    // printf("This system has %d processors configured and "
    //  "%d processors available.\n",
    //  get_nprocs_conf(), get_nprocs());

    int i = 0;

    producerArgument.dir = argv[1];

    for (i = 0; i < threadNo; i++)
    {
        consumerArgument[i].timeWithCountArr = timeWithCountArr;
        consumerArgument[i].threadNo = i + 1;
    }

    pthread_create(&produce, NULL, producer, &producerArgument);

    for (i = 0; i < threadNo; i++)
    {
        pthread_create(&consume[i], NULL, consumer, &consumerArgument[i]);
    }

    pthread_join(produce, NULL);

    for (i = 0; i < threadNo; i++)
    {
        pthread_join(consume[i], NULL);
    }
    clock_t finishReading = clock();
 
    //printf("escape from multithread\n");
    
    int k = atoi(argv[3]);

    TopK(timeWithCountArr, k, number_of_hours); 

    puts("Top K frequently accessed hour:");

    for (int i = number_of_hours - 1; i > number_of_hours - k - 1; i--)
    {
        // convert to date
        struct tm forTimeStampConvertion;
        time_t beforeConvert = (timeWithCountArr[i].hours * 3600) + start_time;

        char storeTime[100];

        forTimeStampConvertion = *localtime(&beforeConvert);
        strftime(storeTime, sizeof(storeTime), "%a %b %e %H:%M:%S %Y", &forTimeStampConvertion);
        // use %Y instead of %G because of ISO 8601 week number calendar <-solve test case 3 problem (year 2023 -> 2022)
        // ref:https://stackoverflow.com/questions/38102558/why-does-datetimes-strftime-give-the-wrong-year-when-i-subtract-days-from-dates
        // time convertion ref: https://www.epochconverter.com/programming/c

        // time convertion end

        printf("%s\t%d\n", storeTime, timeWithCountArr[i].count); // cout timestamp and its count
    }

    clock_t end = clock();
    printf("time used: %f\n", (double)(end - start) / CLOCKS_PER_SEC);
    printf("time used for file reading: %f\n", (double)(finishReading - start) / CLOCKS_PER_SEC);
    // printf("should be dynamic array's size: %d\n\n",currentMaxIdx);
    free(timeWithCountArr);

    return 0;
}
