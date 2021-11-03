#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <dirent.h>
#include <string.h>
#include "tasks.h"
#include "utils.h"
#define MAX_LEN (1024*1024)

int byte;
int rank;

void worker_receive(int num_map_worker, long nbytes, int num_files)
{
	long char_per_worker = nbytes / num_map_workers;
	int remainder = (int)(nbytes % num_map_workers);
	int size;
	MPI_Status status;
	if (rank <= remainder) {
		size = char_per_worker + 2;
	}
	else {
		size = char_per_worker + 1;
	}
	char** chars = (char **)malloc(sizeof(char*) * num_files);
	for (int idx = 0; idx < num_files; idx++) {
		char* msg = (char*)malloc(sizeof(char) * MAX_LEN);
		chars[idx] = msg;
	}
	for (int idx = 0; idx < num_files; idx++) {
		MPI_Recv(chars[idx], size, MPI_CHAR, 0, idx, MPI_COMM_WORLD, &status);
	}
}

void master_map_distribute(char* file, int num_map_workers, int file_idx, long nbytes)
{
	long char_per_worker = nbytes / num_map_workers;
	int remainder = (int)(nbytes % num_map_workers);
	long buffer = 0;
	for (int w = 1; i <= num_map_worker; i++) {
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
		chars[size] = '\0';
		buffer += size;
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

	// Distinguish between master, map workers and reduce workers
	if (rank == 0) {
		// TODO: Implement master process logic
		DIR *d = opendir(input_files_dir);
		struct dirent *dir;
		char* text;
		//Read all files in dir and add to text buffer
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
				master_map_distribute(text, num_map_worker, i, nbytes);

				i++;
			}
			closedir(d);
		}

		while(fgets(buffer, CHUNK, fp)) {
			int worker_num = count % num_map_workers + 1;
			int tag = count / num_map_worker;
			MPI_Send(buffer, size, MPI_CHAR, worker_num, tag, MPI_COMM_WORLD);
			count++;
		}
		printf("Rank (%d): This is the master process\n", rank);
	} else if ((rank >= 1) && (rank <= num_map_workers)) {
		// TODO: Implement map worker process logic
		printf("Rank (%d): This is a map worker process\n", rank);
	} else {
		// TODO: Implement reduce worker process logic
		printf("Rank (%d): This is a reduce worker process\n", rank);
	}

	//Clean up
	MPI_Finalize();
	return 0;
}
