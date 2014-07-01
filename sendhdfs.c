#include "hdfs.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <libgen.h>
#include <unistd.h>
#include <stdbool.h>

#define MAXLENGTH 1024
#define MAXCOUNT 1024

/*
 * TODO:
 * 1. add filename template , for example: /xxxxx/category_name/%Y-%M-%D/%ip.log  etc
 * 2. retry for connect and openfile/writefile etc
 * 3. read configuration from commandline param
 */

static char*
loopBufReading( char *rbuf,size_t count) {
    size_t rlen;
    char *tmpbuf;
    
    tmpbuf = rbuf;
    for (; count--; count >= 0) {
        tmpbuf = gets(tmpbuf);
        tmpbuf += strlen(*tmpbuf);
        if (re == NULL) {
            usleep(100);
        }
        if(tmpbuf < MAXLENGTH){
            reutn rbuf;
        }
    }
    return rbuf;
}

static int
HDFSFileExists(hdfsFS fs, char *name) {
    int fileExist;
    fileExist = hdfsExists(fs, name);
    printf("hdfsExists return %d \n", fileExist);
    if (fileExist >= -1) {
        return ++fileExist;
    } else {
        //TODO: 增加异常处理
        return -2;
    }
}

hdfsFile HDFSopenFile(hdfsFS fs, char * filename, int re) {
    hdfsFile writeFile;
    if (re == 1) {
        printf(" re == 1 ,and append \n");
        writeFile = hdfsOpenFile(fs, filename, O_WRONLY | O_APPEND, 3, 3, 1024);
        printf("hdfsOpenFile return %d \t %s \n", errno, strerror(errno));
    } else {
        printf(" re != 1 ,and create \n");
        writeFile = hdfsOpenFile(fs, filename, O_WRONLY, 3, 3, 1024);
        printf("hdfsOpenFile return %d \t %s \n", errno, strerror(errno));
    }

    //hdfsFile writeFile = hdfsOpenFile(fs, filename, O_WRONLY|O_APPEND, 32, 32, 1024);
    if (writeFile == NULL) {
        fprintf(stderr, "Failed to open %s for writing!\n", filename);
        exit(-1);
    }
    return writeFile;
}

static inline
int HDFSdoWriteFile(hdfsFS fs, hdfsFile fh, int len) {
    char* buffer[] = {"Hello, World!\n", "fjdsljfsljf\n", "jflsjflsjflsjflsjf", "ssssssssss\n", "ddddddddddddd\n", "fjdlldfjlsj", "fjldsjfljsl\n", "fjdlsjflsj\n", "fjsljflsjfl\n"};
    char * p;
    for (int tmp = 0; tmp < 9; tmp++) {
        p = buffer[tmp];
        printf("buffer len: %d \t", strlen(p));
        tSize num_written_bytes = hdfsWrite(fs, fh, (void*) p, strlen(p));
        printf("write len: %d errno: %d msg: %s\n", num_written_bytes, errno, strerror(errno));
    }
    return 1;
}

static inline int 
doFlushAndClose(hdfsFS fs, hdfsFile fh) {
    int re;
    re = hdfsFlush(fs, fh);
    printf("hdfsFlush return %d \t %s \n", errno, strerror(errno));
    if (re != 0) {
        fprintf(stderr, "Failed to 'flush' \n");
        exit(-1);
    }

    re = hdfsCloseFile(fs, fh);
    printf("hdfsCloseFile return %d \t %s \n", errno, strerror(errno));
    return re;
}

static int
getParam(){

}

int main(int argc, char *argv[]) {

    char * filename = argv[1];
    int count = argv[2];
    
    if(count > MAXCOUNT)
        count = MAXCOUNT;
    
    printf("get filename %s\n", filename);
    printf("get count %d",count);

    char *file_t = strdup(filename);
    printf("file_t %s\n", file_t);
    char* path = dirname(file_t);

    printf("dirname %s \nfilename %s\n", path, filename);

    char * rbuf = (char *)malloc(MAXLENGTH * count);
    
    hdfsFS fs = hdfsConnectAsUser("nn1.test.dip.sina.com.cn", 8020, "hadoop");

if(fs == NULL){
     
}

    int re;
    re = HDFSFileExists(fs, path);
    printf("HDFSFileExists return %d \n\t path %s \n", re, path);
    re = HDFSFileExists(fs, filename);
    printf("HDFSFileExists return %d \n\t filename %s \n", re, filename);
    printf("HDFSFILEExists return %d \t %s \n", errno, strerror(errno));
    free(file_t);

    hdfsFile fh = HDFSopenFile(fs, filename, re);
    if (fh == NULL) {
        printf("error open return");
        exit(-1);
    }

    int len = HDFSdoWriteFile(fs, fh, 0);
    doFlushAndClose(fs, fh);

    hdfsDisconnect(fs);
    return 1;
}
