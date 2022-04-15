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

/*
current status:
multithread in single file reading -> performance dropped greatly
multithread on multiple file reading -> perfrormance dropped
multithread heapify (divide in to theadnum works) -> implementing
producer-consumer model -> multithread, 一個thread 讀file (唔做parse)(producer), 另外幾個就負責consume(parse and count).
*/
struct pair time_with_count;  // pair<int,int> in c++
long start_time = 1645491600; // 2022年2月22日Tuesday 01:00:00
long end_time = 1679046032;
int ReadCount = 0;
typedef struct threadargument threadArgument;

struct pair
{
    int hours;
    int count;
};

struct threadargument
{
    int start;
    int end;
    struct pair *timeWithCountArr;
};

int isFile(const char *name)
{
    DIR *directory = opendir(name);

    if (directory != NULL)
    {
        closedir(directory);
        return 0;
    }

    if (errno == ENOTDIR)
    {
        return 1;
    }

    return -1;
}

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
    { // left child's hour bigger than current(also index < size)
        check = left_idx;
    }

    if (right_idx < size && a[check].count == a[right_idx].count && a[check].hours < a[right_idx].hours)
    { // right child's hour bigger than current
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

void *multiThreadDoSth(void *arg)
{
    threadArgument *argStuff = (threadArgument *)arg;
}

void *producer(void *arg)
{
}

void *consumer(void *arg)
{
}

long get_file_length(const char *file_name)
{
    struct stat sb;
    if (stat(file_name, &sb) == -1)
    {
        exit(-1);
    }
    return sb.st_size;
}

int main(int argc, char **argv) //**argv = console command, e.g (./file.c 1 2)
{
    clock_t start = clock();

    int number_of_hours = ((end_time / 3600 * 3600) - start_time) / 3600 + 1;
    struct pair *timeWithCountArr = calloc(number_of_hours, sizeof(struct pair)); // dynamic array

    DIR *FD; // directory pointer
    struct dirent *in_file;
    FILE *common_file; // read file in folder
    char target_file[255];

    int buffer_size = 40;
    char buffer[buffer_size + 1];

    if (NULL == (FD = opendir(argv[1])))
    {
        fprintf(stderr, "Error : Failed to open input directory - %s\n", strerror(errno));
        fclose(common_file);
        return 1;
    }

    while ((in_file = readdir(FD)) != NULL) 
    {
        if (!strcmp(in_file->d_name, ".")) 
            continue;
        if (!strcmp(in_file->d_name, ".."))
            continue;                       
        strcpy(target_file, argv[1]);
        strcat(target_file, in_file->d_name); 

        common_file = fopen(target_file, "r"); 

        if (common_file == NULL)
        {
            fprintf(stderr, "Error : Failed to open entry file - %s\n", strerror(errno));
            fclose(common_file);
            return 1;
        }

        while (fgets(buffer, buffer_size, common_file) != NULL)
        { 
            char *temp;
            long time_stamp = strtol(buffer, &temp, 10); 
            int transformedHour = (time_stamp - start_time) / 3600;

            timeWithCountArr[transformedHour].hours = transformedHour;
            timeWithCountArr[transformedHour].count += 1;
            

        }


        fclose(common_file);
    }


    //用heap sort 出 top k
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
        //ref:https://stackoverflow.com/questions/38102558/why-does-datetimes-strftime-give-the-wrong-year-when-i-subtract-days-from-dates
        //time convertion ref: https://www.epochconverter.com/programming/c

        // time convertion end

        printf("%s\t%d\n", storeTime, timeWithCountArr[i].count); // cout timestamp and its count
        // printf(" its index in the array: %d\n", timeWithCountArr[i].hours);
    }

    clock_t end = clock();
    //printf("time used: %f\n", (double)(end - start) / CLOCKS_PER_SEC);
    //printf("should be dynamic array's size: %d\n\n",currentMaxIdx);
    free(timeWithCountArr);

    return 0;
}
