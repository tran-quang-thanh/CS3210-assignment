#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <dirent.h>
#include <string.h>
#include "tasks.h"
#include "utils.h"

int byte;
int rank;

void worker_receive(char** chars, int num_map_workers, long* file_sizes, int num_files)
{
	int size;
	MPI_Status status;
	chars = (char **)malloc(sizeof(char*) * num_files);
	for (int idx = 0; idx < num_files; idx++) {
		long char_per_worker = file_sizes[idx] / num_map_workers;
		int remainder = (int)(file_sizes[idx] % num_map_workers);
		if (rank <= remainder) {
			size = char_per_worker + 2;
		}
		else {
			size = char_per_worker + 1;
		}
		chars[idx] = (char*)malloc(size);
	}
	for (int idx = 0; idx < num_files; idx++) {
		long char_per_worker = file_sizes[idx] / num_map_workers;
		int remainder = (int)(file_sizes[idx] % num_map_workers);
		if (rank <= remainder) {
			MPI_Recv(chars[idx], char_per_worker+2, MPI_CHAR, 0, idx, MPI_COMM_WORLD, &status);
		}
		else {
			MPI_Recv(chars[idx], char_per_worker+1, MPI_CHAR, 0, idx, MPI_COMM_WORLD, &status);
		}
	}
}

void send_map_list(MapTaskOutput* map_result, int* map_list, int num_map_workers, int num_reduce_workers)
{
	memset(map_list, 0, num_reduce_workers * sizeof(int));
	for (int i = 0; i < map_result->len; i++) {
		int red = partition((map_result->kvs)[i].key, num_reduce_workers);
		map_list[red] += (map_result->kvs)[i].val;
	}
	for (int i = 0; i < num_reduce_workers; i++) {
		MPI_Send(&(map_list[i]), 1, MPI_INT, num_map_workers+i+1, 0, MPI_COMM_WORLD);
	}
}

void receive_map_list(int* reduce_count, int num_map_workers)
{
	MPI_Status status;
	for (int i = 1; i <= num_map_workers; i++) {
		MPI_Recv(&(reduce_count[i]), 1, MPI_INT, i, 0, MPI_COMM_WORLD, &status);
	}
}

void master_map_distribute(char* file, int num_map_workers, int file_idx, long nbytes)
{
	long char_per_worker = nbytes / num_map_workers;
	int remainder = (int)(nbytes % num_map_workers);
	long buffer = 0;
	for (int w = 1; w <= num_map_workers; w++) {
		char* chars;
		int size;
		if (w <= remainder) {
			size = char_per_worker + 2;
		}
		else {
			size = char_per_worker + 1;
		}
		chars = (char *)malloc(size);
		memcpy(file, &chars[buffer], size-1);
		chars[size-1] = '\0';
		buffer += (size-1);
		MPI_Send(chars, size, MPI_CHAR, w, file_idx, MPI_COMM_WORLD); 
	}
}

int main(int argc, char** argv) {
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
	MapTaskOutput* (*map) (char*);
	switch(map_reduce_task_num){
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

	// Distinguish between master, map workers and reduce workers
	if (rank == 0) {
		// TODO: Implement master process logic
		DIR *d = opendir(input_files_dir);
		struct dirent *dir;
		char* text;

		// Read all file size and send to worker
		if (d) {
			int i = 0;
			while(i < num_files && (dir = readdir(d)) != NULL) {
				// Read text file
				char *filename = dir->d_name;
				printf("%s\n", filename);
				FILE *fp = fopen(filename, "r");
				fseek(fp, 0, SEEK_END);
				long nbytes = ftell(fp);
				fseek(fp, 0, SEEK_SET);
				fclose(fp);

				// Set file size to array
				file_sizes[i] = nbytes;
				i++;
			}
			closedir(d);
		}
		// Broadcast all file sizes to other processes
		MPI_Bcast(file_sizes, num_files, MPI_LONG, 0, MPI_COMM_WORLD);

		// Read all files in dir and add to text buffer
		if (d) {
			int i = 0;
			while(i < num_files && (dir = readdir(d)) != NULL) {
				char *filename = dir->d_name;
				printf("%s\n", filename);
				FILE *fp = fopen(filename, "r");
				fseek(fp, 0, SEEK_END);
				long nbytes = ftell(fp);
				fseek(fp, 0, SEEK_SET);
				text = (char*)malloc(nbytes+1);
				fread(text, (size_t)nbytes, 1, fp);
				text[nbytes] = '\0';
				fclose(fp);

				// Distribute the file to all workers
				master_map_distribute(text, num_map_workers, i, nbytes);
				i++;
			}
			closedir(d);
		}

		printf("Rank (%d): This is the master process\n", rank);
	} else if ((rank >= 1) && (rank <= num_map_workers)) {
		// TODO: Implement map worker process logic
		// Receive file sizes array from master
		MPI_Bcast(file_sizes, num_files, MPI_LONG, 0, MPI_COMM_WORLD);

		// Receive distributed text from master
		char** chars;
		worker_receive(chars, num_map_workers, file_sizes, num_files);

		// Calculate output
		char* char_sum = chars[0];
		for (int i = 1; i < num_files; i++) {
			strcat(char_sum, " ");
			strcat(char_sum, chars[i]);
		}
		MapTaskOutput* map_result = (*map)(char_sum);

		// Send map list to reducer
		int* map_list;
		send_map_list(map_result, map_list, num_map_workers, num_reduce_workers);

		printf("Rank (%d): This is a map worker process\n", rank);
	} else {
		// TODO: Implement reduce worker process logic

		// Receive map list from map worker
		int reduce_count[num_map_workers+1];
		receive_map_list(reduce_count, num_map_workers);

		printf("Rank (%d): This is a reduce worker process\n", rank);
	}

	//Clean up
	MPI_Finalize();
	return 0;
}
