#include <stdio.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

struct __attribute__((__packed__)) log_t {
	int64_t log_id;
	int64_t term;
	int32_t msg_sz;
	int64_t cluster_id;
	char msg[0];
};

int main()
{
	int err = 0;
	int fd = 0;
	int buf_size = 0;
	char *buf = NULL;
	struct stat statbuf;
	const char *war_file = "/Users/wenlinwu/src/nebula-walcheck/data/storaged.0/nebula/1/wal/1/0000000000000000001.wal";
	struct log_t *logp = NULL;

	printf("log size: %lu\n", sizeof(struct log_t));

	if ((fd = open(war_file, O_RDWR)) < 0) {
		perror("error openning file");
		return -1;
	}

	printf("fd: %d\n", fd);

	if ((err = fstat(fd, &statbuf)) < 0) {
		perror("failed stat file");
		return -1;
	}

	buf_size = statbuf.st_size;
	printf("file size: %d\n", buf_size);
	if ((buf = mmap(NULL, statbuf.st_size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0)) == MAP_FAILED) {
		perror("failed mmap()");
		return -1;
	}

	logp = (struct log_t *) buf;
	while (buf_size > 0) {
		int32_t *foot = NULL;
		int log_sz = sizeof(struct log_t) + logp->msg_sz + 4; // foot size = 4
		buf_size -= log_sz;
		foot = (int *) (logp->msg + logp->msg_sz);

		printf("log id: %lld, term: %lld, msg size: %d, cluster id: %lld, foot: %d, buf sz: %d\n", logp->log_id, logp->term, logp->msg_sz, logp->cluster_id, *foot, buf_size);
		logp = (struct log_t *) (&foot[1]);
	}

	return 0;
}
