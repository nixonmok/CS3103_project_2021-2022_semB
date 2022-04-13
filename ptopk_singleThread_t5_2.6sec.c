#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <dirent.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <time.h>

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

int main(int argc, char **argv) //**argv 應該係讀console command,姐係(./file.c 1 2) 呢啲
{
    DIR *FD; // directory pointer
    struct dirent *in_file;
    FILE *common_file; // read file in folder
    char target_file[255];

    int input_is_file = isFile(argv[1]); // read console command ./test.o "case5/"<-this 1645491600 5
    struct pair time_with_count;         // pair<int,int> in c++

    int buffer_size = 40;
    char buffer[buffer_size + 1];
    long start_time = 1645491600; // 2022年2月22日Tuesday 01:00:00
    long end_time = 1679046032;

    int number_of_hours = ((end_time / 3600 * 3600) - start_time) / 3600; //由start time 去到 end time有幾多個hours
    //答案係9320個,所以counter_array最多會係9320 * int byte 咁大 唔知會唔會leak

    int max_entry = 2 << 25;                                                       // 2 leftshift 25 bit, = 2 * 2^25
    struct pair *timeWithCountArr = malloc(number_of_hours * sizeof(struct pair)); // dynamic array, 屌佢老母乜9都唔教



    if (input_is_file == 1)
    {
    //    printf("it's a file\n"); 
    //testing print
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

        while ((in_file = readdir(FD)) != NULL) //總之係讀directory
        {
            if (!strcmp(in_file->d_name, ".")) //directory第一個file係"dir/.""
                continue;
            if (!strcmp(in_file->d_name, "..")) //directory第二個file係"dir/.."
                continue;                       //所以continue skip咗佢(冇用)
            strcpy(target_file, argv[1]);
            strcat(target_file, in_file->d_name); // 組合兩個 string, argv[1] = "case5/", in_file -> d_name = "input名"

            common_file = fopen(target_file, "r"); //開file

            if (common_file == NULL)
            {
                fprintf(stderr, "Error : Failed to open entry file - %s\n", strerror(errno));
                fclose(common_file);
                return 1;
            }

            int line = 0;
            int counter = 0;

            //呢個係single thread read and parse搬過黎,諗下點multithread
            while (fgets(buffer, buffer_size, common_file) != NULL)
            { // parse嘅內容
                char *temp;
                long time_stamp = strtol(buffer, &temp, 10); //將char*,姐係buffer由頭開始讀,讀到唔係數字爲止, return long int base n, temp 會變做指去第一個唔係數字嘅位
                int transformedHour = (time_stamp - start_time) / 3600;

                timeWithCountArr[transformedHour].hours = transformedHour;
                timeWithCountArr[transformedHour].count += 1;
                //&temp 停咗係個逗號到,未寫讀佢嘅code

                counter++;
            }

            // body結束

            fclose(common_file);
        }
        //讀完所有file, 開始topk algorithm
            int max = 0;
            int maximum_hour;
            //用heap sort 出 top k
            TopK(timeWithCountArr, 5, number_of_hours); //用pair,轉咗位搵得返個timestamp

            for (int i = number_of_hours - 1; i > number_of_hours - 5 - 1; i--)
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
            printf("time take: %f seconds\n\n",(double)(end - start)/CLOCKS_PER_SEC);

            free(timeWithCountArr);
    }

    return 0;
}
