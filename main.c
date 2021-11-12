#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <dirent.h>
#include <string.h>
#include <stdbool.h>
#include "tasks.h"
#include "utils.h"

int byte;
int rank;

void worker_receive(char **chars, int num_map_workers, long *file_sizes, int num_files) {
    int size;
    MPI_Status status;

    for (int idx = 0; idx < num_files; idx++) {
        long char_per_worker = file_sizes[idx] / num_map_workers;
        int remainder = (int) (file_sizes[idx] % num_map_workers);
        if (rank <= remainder) {
            size = char_per_worker + 2;
        } else {
            size = char_per_worker + 1;
        }
        chars[idx] = (char *) malloc(size);
    }
    for (int idx = 0; idx < num_files; idx++) {
        long char_per_worker = file_sizes[idx] / num_map_workers;
        int remainder = (int) (file_sizes[idx] % num_map_workers);
        if (rank <= remainder) {
            MPI_Recv(chars[idx], char_per_worker + 2, MPI_CHAR, 0, idx, MPI_COMM_WORLD, &status);
        } else {
            MPI_Recv(chars[idx], char_per_worker + 1, MPI_CHAR, 0, idx, MPI_COMM_WORLD, &status);
        }
        //printf("Rank = %d\n", rank);
        //printf("%s\n", chars[idx]);
    }
}

void send_map_list(MapTaskOutput *map_result, int *map_list, int num_map_workers, int num_reduce_workers) {
    for (int i = 0; i < map_result->len; i++) {
        int reduce = partition((map_result->kvs)[i].key, num_reduce_workers);
        map_list[reduce]++;
    }

    for (int i = 0; i < num_reduce_workers; i++) {
        MPI_Send(&(map_list[i]), 1, MPI_INT, num_map_workers + i + 1, 0, MPI_COMM_WORLD);
    }

    // Index of current key-val pair
    int indices[num_reduce_workers];
    for (int i = 0; i < num_reduce_workers; i++) {
        indices[i] = 0;
    }

    // All keys mapping to each reduce workers
    char ***keys = (char ***) malloc(num_reduce_workers * sizeof(char **));

    // All vals mapping to each reduce workers
    int **vals = (int **) malloc(num_reduce_workers * sizeof(int *));

    for (int i = 0; i < num_reduce_workers; i++) {
        keys[i] = (char **) malloc(map_list[i] * sizeof(char *));
        for (int j = 0; j < map_list[i]; j++) {
            keys[i][j] = (char *) malloc(8 * sizeof(char));
        }
        vals[i] = (int *) malloc(map_list[i] * sizeof(int));
    }

    // Set keys and vals array, update indices when seeing a partition belongs to that reduce worker
    for (int i = 0; i < map_result->len; i++) {
        int reduce = partition((map_result->kvs)[i].key, num_reduce_workers);
        int idx = indices[reduce];
        keys[reduce][idx] = (map_result->kvs)[i].key;
        vals[reduce][idx] = (map_result->kvs)[i].val;
        indices[reduce]++;
    }

    // Send key-val list to reduce workers
    for (int i = 0; i < num_reduce_workers; i++) {
        for (int j = 0; j < map_list[i]; j++) {
            MPI_Send(keys[i][j], 8, MPI_CHAR, num_map_workers + i + 1, 2 * i + j + 1, MPI_COMM_WORLD);
        }
        MPI_Send(vals[i], map_list[i], MPI_INT, num_map_workers + i + 1, 2 * i + map_list[i] + 1, MPI_COMM_WORLD);
        printf("from = %d, to = %d, size = %d\n", rank, num_map_workers + i + 1, map_list[i]);
        /*
        for (int j = 0; j < map_list[i]; j++) {
            printf("%s %d\n", keys[i][j], vals[i][j]);
        }
        */
    }
}

void receive_map_list(int *reduce_count, char ***keys, int **vals, int num_map_workers) {
    MPI_Status status;
    for (int i = 0; i < num_map_workers; i++) {
        MPI_Recv(&(reduce_count[i]), 1, MPI_INT, i + 1, 0, MPI_COMM_WORLD, &status);
    }

    for (int i = 0; i < num_map_workers; i++) {
        keys[i] = (char **) malloc(reduce_count[i] * sizeof(char *));
        for (int j = 0; j < reduce_count[i]; j++) {
            keys[i][j] = (char *) malloc(8 * sizeof(char));
        }
        vals[i] = (int *) malloc(reduce_count[i] * sizeof(int));
    }

    for (int i = 0; i < num_map_workers; i++) {
        int reduce_idx = rank - (num_map_workers + 1);
        for (int j = 0; j < reduce_count[i]; j++) {
            MPI_Recv(keys[i][j], 8, MPI_CHAR, i + 1, 2 * reduce_idx + j + 1, MPI_COMM_WORLD, &status);
        }
        MPI_Recv(vals[i], reduce_count[i], MPI_INT, i + 1, 2 * reduce_idx + reduce_count[i] + 1, MPI_COMM_WORLD,
                 &status);
        /*
        printf("receive = %d, send = %d size = %d\n", rank, i+1, reduce_count[i]);
        for (int j = 0; j < reduce_count[i]; j++) {
            printf("%s %d\n", keys[i][j], vals[i][j]);
        }
        */
    }
}

void reduce_calculate_send(int *reduce_count, char ***keys, int **vals, int num_map_workers) {
    int count = 0;
    for (int i = 0; i < num_map_workers; i++) {
        count += reduce_count[i];
    }
    char **key_set = (char **) malloc(count * sizeof(char *));
    for (int i = 0; i < count; i++) {
        key_set[i] = (char *) malloc(8 * sizeof(char));
    }
    int *val_set = (int *) malloc(count * sizeof(int));
    int len = 0;
    for (int i = 0; i < num_map_workers; i++) {
        char **key_worker = keys[i];
        int *val_worker = vals[i];
        bool contains = false;
        for (int j = 0; j < reduce_count[i]; j++) {
            // if key is in key output set, modify val output set
            for (int k = 0; k < len; k++) {
                if (strcmp(key_set[k], key_worker[j]) == 0) {
                    contains = true;
                    val_set[k] += val_worker[j];
                    break;
                }
            }
            // else add key to key output set
            if (contains == false) {
                key_set[len] = key_worker[j];
                val_set[len] = val_worker[j];
                len++;
            }
        }
    }
    /*
    for (int i = 0; i < len; i++) {
        printf("%s %d\n", key_set[i], val_set[i]);
    }
    */
    // Send map length to master
    MPI_Send(&len, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
    // Send all keys and values to master
    for (int i = 0; i < len; i++) {
        MPI_Send(key_set[i], 8, MPI_CHAR, 0, 2 * i + 1, MPI_COMM_WORLD);
        MPI_Send(&val_set[i], 1, MPI_INT, 0, 2 * i + 2, MPI_COMM_WORLD);
    }
}

void master_map_distribute(char *file, int num_map_workers, int file_idx, long nbytes) {
    long char_per_worker = nbytes / num_map_workers;
    int remainder = (int) (nbytes % num_map_workers);
    long buffer = 0;
    for (int w = 1; w <= num_map_workers; w++) {
        char *chars;
        long size;
        if (w <= remainder) {
            size = char_per_worker + 2;
        } else {
            size = char_per_worker + 1;
        }
        chars = (char *) malloc(size);
        memcpy(chars, &file[buffer], size - 1);
        chars[size - 1] = '\0';
        //printf("w = %d\n", w);
        //printf("%s\n", chars);
        buffer += (size - 1);
        MPI_Send(chars, size, MPI_CHAR, w, file_idx, MPI_COMM_WORLD);
    }
}

void master_receive_reduce(int num_map_workers, int num_reduce_workers, char *output_file_name) {
    int total_len = 0;
    MPI_Status status;

    // Receive map size of each reduce worker
    int *all_len = (int *) malloc(num_reduce_workers * sizeof(int));
    for (int i = 0; i < num_reduce_workers; i++) {
        MPI_Recv(&all_len[i], 1, MPI_INT, i + 1 + num_map_workers, 0, MPI_COMM_WORLD, &status);
    }
    for (int i = 0; i < num_reduce_workers; i++) {
        total_len += all_len[i];
    }

    // Create buffer for final key-value
    char **key_set = (char **) malloc(total_len * sizeof(char *));
    for (int i = 0; i < total_len; i++) {
        key_set[i] = (char *) malloc(8 * sizeof(char));
    }
    int *val_set = (int *) malloc(total_len * sizeof(int));
    int idx = 0;

    // Set value for final output
    for (int i = 0; i < num_reduce_workers; i++) {
        for (int j = 0; j < all_len[i]; j++) {
            MPI_Recv(key_set[idx], 8, MPI_CHAR, i + 1 + num_map_workers, 2 * j + 1, MPI_COMM_WORLD, &status);
            MPI_Recv(&val_set[idx], 1, MPI_INT, i + 1 + num_map_workers, 2 * j + 2, MPI_COMM_WORLD, &status);
            idx++;
        }
    }

    // Output to file
    FILE *fptr = fopen(output_file_name, "w+");
    fprintf(fptr, "%s %d", key_set[0], val_set[0]);
    for (int i = 1; i < total_len; i++) {
        fprintf(fptr, "\n%s %d", key_set[i], val_set[i]);
    }
}

int main(int argc, char **argv) {
    MPI_Init(&argc, &argv);

    int world_size;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    // Get command-line params
    char *input_files_dir = argv[1];
    int num_files = atoi(argv[2]);
    int num_map_workers = atoi(argv[3]);
    int num_reduce_workers = atoi(argv[4]);
    char *output_file_name = argv[5];
    int map_reduce_task_num = atoi(argv[6]);

    // Identify the specific map function to use
    MapTaskOutput *(*map)(char *);
    switch (map_reduce_task_num) {
        case 1:
            map = &map1;
            break;
        case 2:
            map = &map2;
            break;
        case 3:
            map = &map3;
            break;
    }
    long file_sizes[num_files];


    /* init for the MPI struct */
    const int nitems = 2;
    int block_lengths[2] = {8, 1};
    MPI_Datatype types[2] = {MPI_CHAR, MPI_INT};
    MPI_Datatype mpi_key_value;
    MPI_Aint     offsets[2];

    offsets[0] = offsetof(KeyValue, key);
    offsets[1] = offsetof(KeyValue, val);

    MPI_Type_create_struct(nitems, blocklengths, offsets, types, &mpi_key_value);
    MPI_Type_commit(&mpi_key_value);


    // Distinguish between master, map workers and reduce workers
    if (rank == 0) {
        // TODO: Implement master process logic
        struct dirent **all_files;
        int n = scandir(input_files_dir, &all_files, 0, alphasort);
        char *text;
        FILE *fp;

        // Read all file size and send to worker
        if (n) {
            int i = 2;
            while (i < num_files + 2) {
                // Read text file
                char *filename = all_files[i]->d_name;
                char *temp_dir = malloc(100);
                strcpy(temp_dir, input_files_dir);
                strcat(temp_dir, filename);
                fp = fopen(temp_dir, "r");
                if (fp == NULL) {
                    printf("File not found\n");
                } else {
                    fseek(fp, 0, SEEK_END);
                    long nbytes = ftell(fp);
                    fseek(fp, 0, SEEK_SET);

                    //Set file size to array
                    file_sizes[i - 2] = nbytes;
                    fclose(fp);
                }
                i++;
            }
        }

        // Broadcast all file sizes to other processes
        MPI_Bcast(file_sizes, num_files, MPI_LONG, 0, MPI_COMM_WORLD);
        for (int i = 0; i < num_files; i++) {
            printf("%ld\n", file_sizes[i]);
        }

        // Read all files in dir and add to text buffer
        if (n) {
            int i = 0;
            while (i < num_files) {
                // add two to get index because first two files are ./ and ../
                char *filename = all_files[i + 2]->d_name;
                char *temp_dir = malloc(100);
                strcpy(temp_dir, input_files_dir);
                strcat(temp_dir, filename);
                fp = fopen(temp_dir, "r");
                if (fp == NULL) {
                    printf("File not found\n");
                    return 1;
                }
                text = (char *) malloc(file_sizes[i] + 1);
                fread(text, sizeof(char), (size_t) file_sizes[i], fp);
                text[file_sizes[i]] = '\0';
                fclose(fp);

                // Distribute the file to all workers
                master_map_distribute(text, num_map_workers, i, file_sizes[i]);
                i++;
            }
        }

        for (int j = 0; j < n; j++) {
            free(all_files[j]);
        }
        free(all_files);

        master_receive_reduce(num_map_workers, num_reduce_workers, output_file_name);
        //printf("Rank (%d): This is the master process\n", rank);
    } else if ((rank >= 1) && (rank <= num_map_workers)) {
        // TODO: Implement map worker process logic
        // Receive file sizes array from master
        MPI_Bcast(file_sizes, num_files, MPI_LONG, 0, MPI_COMM_WORLD);

        // Receive distributed text from master

        char **chars = (char **) malloc(sizeof(char *) * num_files);
        worker_receive(chars, num_map_workers, file_sizes, num_files);

        // Calculate output
        int sum_length = 0;
        for (int i = 0; i < num_files; i++) {
            sum_length += strlen(chars[i]) + 2;
        }
        char char_sum[sum_length];
        strcpy(char_sum, chars[0]);
        for (int i = 1; i < num_files; i++) {
            strcat(char_sum, chars[i]);
        }
        MapTaskOutput *map_result = (*map)(char_sum);
        //printf("%d\n", map_result->len);
        //for (int i = 0; i < map_result->len; i++) {
        //	printf("%s %d\n", map_result->kvs[i].key, map_result->kvs[i].val);
        //}

        // Send map list to reducer
        int map_list[num_reduce_workers];
        for (int i = 0; i < num_reduce_workers; i++) {
            map_list[i] = 0;
        }
        send_map_list(map_result, map_list, num_map_workers, num_reduce_workers);
        free_map_task_output(map_result);

        //printf("Rank (%d): This is a map worker process\n", rank);
    } else {
        // TODO: Implement reduce worker process logic

        // Receive map list from map worker
        int reduce_count[num_map_workers];

        char ***keys = (char ***) malloc(num_map_workers * sizeof(char **));
        int **vals = (int **) malloc(num_map_workers * sizeof(int *));
        receive_map_list(reduce_count, keys, vals, num_map_workers);
        reduce_calculate_send(reduce_count, keys, vals, num_map_workers);

        //printf("Rank (%d): This is a reduce worker process\n", rank);
    }

    //Clean up
    MPI_Type_free(&mpi_key_value);
    MPI_Finalize();
    return 0;
}