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

struct pair time_with_count;  // pair<int,int> in c++
long start_time = 1645491600; // 2022年2月22日Tuesday 01:00:00
long end_time = 1679046032;

struct pair
{
    int hours;
    int count;
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
    { // left child bigger than current(also index < size)
        check = left_idx;
    }

    if (right_idx < size && a[check].count == a[right_idx].count && a[check].hours < a[right_idx].hours)
    { // right child bigger than current
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
        // heap sort k times -> n - k - 1
        // it is correct because from the main(), the "cout" loop is loop from backward,
        // so I just need to do heap sort for K time(for heap sort, the largest element will
        // move to last position everytime.
    }
}

// multithread reading
struct thread_arg 
{
    FILE *input_file;
    int start;
    int end;
    // int slice_size;
    char header_frag[40];
    char tail_frag[40];
    struct pair *timeWithCountArr;
};

// multi-thread reading
typedef struct thread_arg ThreadArg;

long get_file_length(const char *file_name)
{
    struct stat sb;
    if (stat(file_name, &sb) == -1)
    {
        exit(-1);
    }
    return sb.st_size;
}

void *ReadChunk(void *arg)
{
    ThreadArg *arg_value = (ThreadArg *)arg;
    // jump to target location

    fseek(arg_value->input_file, arg_value->start, SEEK_SET);

    int buffer_size = 40;
    char buffer[buffer_size];
    FILE *input = arg_value->input_file;

    fgets(buffer, buffer_size, input);
    char *temp;
    long time_stamp = strtol(buffer, &temp, 10);

    if (buffer[10] == ',' && buffer[10 - 4] != ',')
    {
        int transformedHour = (time_stamp - start_time) / 3600;

        arg_value -> timeWithCountArr[transformedHour].hours = transformedHour;
        arg_value -> timeWithCountArr[transformedHour].count += 1;
        // counter_array[time_stamp - start_time] += 1;
        //  skip the frag
    }
    else
    {

        strcpy(arg_value->header_frag, buffer);
    }
    int readed = strlen(buffer);
    while (arg_value->start + readed < arg_value->end && fgets(buffer, buffer_size, input) != NULL)
    {
        readed += strlen(buffer);
        time_stamp = strtol(buffer, &temp, 10);
        int transformedHour = (time_stamp - start_time) / 3600;

        arg_value -> timeWithCountArr[transformedHour].hours = transformedHour;
        arg_value -> timeWithCountArr[transformedHour].count += 1;
        // counter_array[time_stamp - start_time] += 1;
    }

    if (arg_value->start + readed > arg_value->end)
    {
        // a fragment is readed, put it into the tail_frag
        strcpy(arg_value->tail_frag, buffer);
    }
}
// multi-thread reading end

int main(int argc, char **argv) //**argv 應該係讀console command,姐係(./file.c 1 2) 呢啲
{

    DIR *FD; // directory pointer
    struct dirent *in_file;
    FILE *common_file; // read file in folder
    char target_file[255];
    int buffer_size = 40;
    char buffer[buffer_size + 1];

    int input_is_file = isFile(argv[1]); // read console command ./test.o "case5/"<-this 1645491600 5

    int number_of_hours = ((end_time / 3600 * 3600) - start_time) / 3600; 

    struct pair *timeWithCountArr = calloc(number_of_hours, sizeof(struct pair)); // use calloc to initialize untouch element to 0

    if (input_is_file == 1)
    {
        //    printf("it's a file\n");
        // testing print
    }
    else if (input_is_file == 0)
    {
        clock_t start = clock();
        //    printf("it's a dir\n");
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

            int thread_num = 2;                                
            long file_len = get_file_length(target_file);      
            long size_for_each_thread = file_len / thread_num; 

            ThreadArg arg_list[thread_num]; 
            pthread_t readers[thread_num]; 

            int start_posi = 0;

            
            for (int i = 0; i < thread_num - 1; i++)
            { 
                arg_list[i].input_file = fopen(target_file, "r");
                arg_list[i].start = start_posi;
                

                start_posi += size_for_each_thread;

                arg_list[i].end = start_posi;                  
                arg_list[i].timeWithCountArr = timeWithCountArr; 
            }

            arg_list[thread_num-1].input_file = fopen(target_file, "r");

            arg_list[thread_num-1].start = start_posi;
            arg_list[thread_num-1].end = file_len;
            arg_list[thread_num-1].timeWithCountArr = timeWithCountArr;

            //儲存完

            for (int i = 0; i < thread_num; i++)
            {
                pthread_create(&readers[i], NULL, ReadChunk, &arg_list[i]);
                
            }
            for (int i = 0; i < thread_num; i++)
            {
                pthread_join(readers[i], NULL);
                
            }

            for (int i = 0; i < thread_num; i++)
            {
                fclose(arg_list[i].input_file);
                
            }           
        }



        
        int max = 0;
        int maximum_hour;
        
        int k = atoi(argv[3]);
        TopK(timeWithCountArr, k, number_of_hours); 

        for (int i = number_of_hours - 1; i > number_of_hours - k - 1; i--)
        {
            // convert to date
            struct tm forTimeStampConvertion;
            time_t beforeConvert = (timeWithCountArr[i].hours * 3600) + start_time;
            char storeTime[100];

            forTimeStampConvertion = *localtime(&beforeConvert);
            strftime(storeTime, sizeof(storeTime), "%a %b %d %H:%M:%S %G", &forTimeStampConvertion);
            // time convertion end

            printf("%s\t%d\n", storeTime, timeWithCountArr[i].count); // cout timestamp and its count
        }

        clock_t end = clock();
        printf("time take: %f seconds\n\n", (double)(end - start) / CLOCKS_PER_SEC);

        free(timeWithCountArr);
    }

    return 0;
}
