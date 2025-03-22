#include "iLSM.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <linux/nvme_ioctl.h>
#include <fcntl.h>
#include <cstring>
#include <cstdint>
#include <stdlib.h>

#include <iostream>
#include <cstdio>
#include <inttypes.h>

using namespace std;


const unsigned int PAGE_SIZE = 4096;
const unsigned int MAX_BUFLEN = 4*1024; // 1MB
const unsigned int NSID = 1;

int iLSM::DB::Open(const std::string &dev)
{
    int err;
    err = open(dev.c_str(), O_RDONLY);
    if (err < 0)
        return -1; // fail to open
    fd_ = err;
    struct stat nvme_stat;
    err = fstat(fd_, &nvme_stat);
    if (err < 0)
        return -1;
    if (!S_ISCHR(nvme_stat.st_mode) && !S_ISBLK(nvme_stat.st_mode))
        return -1;
    return 0;
}

int iLSM::DB::Put(const std::string &key, const std::string &value)
{
    void *data = NULL;
    unsigned int data_len = value.size();
    unsigned int nlb = ((data_len-1)/PAGE_SIZE); 
    data_len = (nlb+1) * PAGE_SIZE;
    if (posix_memalign(&data, PAGE_SIZE, data_len)) {
        return -ENOMEM;
    }
    memcpy(data, value.c_str(), value.size());
    int err;
    uint32_t result;
    uint32_t cdw2, cdw3, cdw10, cdw11, cdw12, cdw13, cdw14, cdw15;
    cdw2 = cdw3 = cdw10 = cdw11 = cdw12 = cdw13 = cdw14 = cdw15 = 0;
    //    memcpy(&cdw10, key.c_str()+4, 4);
    //    memcpy(&cdw11, key.c_str(), 4);
    memcpy(&cdw10, key.c_str(), 4);
    cdw12 = 0 | (0xFFFF & nlb);
    cdw13 = value.size();
    err = nvme_passthru(NVME_CMD_KV_PUT, 0, 0, NSID, cdw2, cdw3, 
            cdw10, cdw11, cdw12, cdw13, cdw14, cdw15, 
            data_len, data, result);
    free(data);
    if (err < 0) {
        return -1;
    } else if (result != 0) {
        return -1;
    }
    return 0;
}

int iLSM::DB::Seek(const unsigned int iter_id, const std::string &key, std::string &value)
{
    void *data = NULL;
    unsigned int data_len = MAX_BUFLEN;
    unsigned int nlb = (MAX_BUFLEN-1) / PAGE_SIZE;
    if (posix_memalign(&data, PAGE_SIZE, data_len)) {
        return -ENOMEM;
    }
    memset(data, 0, data_len);
    int err;
    uint32_t result;
    uint32_t cdw2, cdw3, cdw10, cdw11, cdw12, cdw13, cdw14, cdw15;
    cdw2 = cdw3 = cdw10 = cdw11 = cdw12 = cdw13 = cdw14 = cdw15 = 0;
    //    memcpy(&cdw10, key.c_str()+4, 4);
    //    memcpy(&cdw11, key.c_str(), 4);
    memcpy(&cdw10, key.c_str(), 4);
    cdw12 = 0 | (0xFFFF & nlb);
    cdw13 = iter_id;
    err = nvme_passthru(NVME_CMD_KV_ITER_SEEK, 0, 0, NSID, cdw2, cdw3, 
            cdw10, cdw11, cdw12, cdw13, cdw14, cdw15, 
            data_len, data, result);

    if (err < 0) {
        // ioctl fail
        perror("ilsm seek");
        free(data);
        return -1;
    }

    if (err == 0x7C1) {
        // no such key
        free(data);
        return -2;
    }

    if (result > 0) { // value length
        value = std::string(static_cast<const char*>(data), (int)4);
    }  else {
        value = std::string();
    }
    free(data);
    return result;
}

int iLSM::DB::Next(const unsigned int iter_id, std::string &value)
{
    void *data = NULL;
    unsigned int data_len = MAX_BUFLEN;
    unsigned int nlb = (MAX_BUFLEN-1) / PAGE_SIZE;
    std::string key="1234";
    if (posix_memalign(&data, PAGE_SIZE, data_len)) {
        return -ENOMEM;
    }
    memset(data, 0, data_len);
    int err;
    uint32_t result;
    uint32_t cdw2, cdw3, cdw10, cdw11, cdw12, cdw13, cdw14, cdw15;
    cdw2 = cdw3 = cdw10 = cdw11 = cdw12 = cdw13 = cdw14 = cdw15 = 0;
    //    memcpy(&cdw10, key.c_str()+4, 4);
    //    memcpy(&cdw11, key.c_str(), 4);
    cdw10 = 0; // key
    cdw12 = 0 | (0xFFFF & nlb);
    cdw13 = iter_id;
    err = nvme_passthru(NVME_CMD_KV_ITER_NEXT, 0, 0, NSID, cdw2, cdw3, 
            cdw10, cdw11, cdw12, cdw13, cdw14, cdw15, 
            data_len, data, result);

    if (err < 0) {
        // ioctl fail
        perror("ilsm next");
        free(data);
        return -1;
    }

    if (err == 0x7C1) {
        // no such key
        free(data);
        return -2;
    }

    if (result > 0) { // value length
        value = std::string(static_cast<const char*>(data), (int)4);
    }  else {
        value = std::string();
    }

    free(data);
    return result;
}


int iLSM::DB::Get(const std::string &key, std::string &value)
{
    void *data = NULL;
    unsigned int data_len = MAX_BUFLEN;
    unsigned int nlb = (MAX_BUFLEN-1) / PAGE_SIZE;
    if (posix_memalign(&data, PAGE_SIZE, data_len)) {
        return -ENOMEM;
    }
    memset(data, 0, data_len);
    int err;
    uint32_t result;
    uint32_t cdw2, cdw3, cdw10, cdw11, cdw12, cdw13, cdw14, cdw15;
    cdw2 = cdw3 = cdw10 = cdw11 = cdw12 = cdw13 = cdw14 = cdw15 = 0;
    //    memcpy(&cdw10, key.c_str()+4, 4);
    //    memcpy(&cdw11, key.c_str(), 4);
    memcpy(&cdw10, key.c_str(), 4);
    cdw12 = 0 | (0xFFFF & nlb);
    err = nvme_passthru(NVME_CMD_KV_GET, 0, 0, NSID, cdw2, cdw3, 
            cdw10, cdw11, cdw12, cdw13, cdw14, cdw15, 
            data_len, data, result);

    if (err < 0) {
        // ioctl fail
        perror("ilsm get");
        free(data);
        return -1;
    }

    if (err == 0x7C1) {
        // no such key
        free(data);
        return -2;
    }

    if (result > 0) { // value length
        value = std::string(static_cast<const char*>(data), (int)result);
    }  else {
        value = std::string();
    }

    free(data);
    return result;
}

int iLSM::DB::CreateIter(unsigned int &iter_id)
{
    void *data = NULL;
    unsigned int data_len = MAX_BUFLEN;
    unsigned int nlb = (MAX_BUFLEN-1) / PAGE_SIZE;
    if (posix_memalign(&data, PAGE_SIZE, data_len)) {
        return -ENOMEM;
    }
    memset(data, 0, data_len);
    int err;
    uint32_t result;
    uint32_t cdw2, cdw3, cdw10, cdw11, cdw12, cdw13, cdw14, cdw15;
    cdw2 = cdw3 = cdw10 = cdw11 = cdw12 = cdw13 = cdw14 = cdw15 = 0;
    //    memcpy(&cdw10, key.c_str()+4, 4);
    cdw12 = 0 | (0xFFFF & nlb);
    err = nvme_passthru(NVME_CMD_KV_ITER_CREATE_ITER, 0, 0, NSID, cdw2, cdw3, 
            cdw10, cdw11, cdw12, cdw13, cdw14, cdw15, 
            data_len, data, result);
    free(data);
    if (err < 0) {
        // ioctl fail
        perror("ilsm create iter");
        return -1;
    }

    if (err == 0x7C1) {
        // no such key
        return -2;
    }

    iter_id = result;
    return 0;
}

int iLSM::DB::DestroyIter(const unsigned int iter_id)
{
    void *data = NULL;
    unsigned int data_len = MAX_BUFLEN;
    unsigned int nlb = (MAX_BUFLEN-1) / PAGE_SIZE;
    if (posix_memalign(&data, PAGE_SIZE, data_len)) {
        return -ENOMEM;
    }
    memset(data, 0, data_len);
    int err;
    uint32_t result;
    uint32_t cdw2, cdw3, cdw10, cdw11, cdw12, cdw13, cdw14, cdw15;
    cdw2 = cdw3 = cdw10 = cdw11 = cdw12 = cdw13 = cdw14 = cdw15 = 0;
    //    memcpy(&cdw10, key.c_str()+4, 4);
    //    memcpy(&cdw11, key.c_str(), 4);
    cdw12 = 0 | (0xFFFF & nlb);
    cdw13 = iter_id;
    err = nvme_passthru(NVME_CMD_KV_ITER_DESTROY_ITER, 0, 0, NSID, cdw2, cdw3, 
            cdw10, cdw11, cdw12, cdw13, cdw14, cdw15, 
            data_len, data, result);
    free(data);
    if (err < 0) {
        // ioctl fail
        perror("ilsm destroy iter");
        return -1;
    }

    if (err == 0x7C1) {
        // no such key
        return -2;
    }

    return result;
}

int iLSM::DB::nvme_passthru(uint8_t opcode,
        uint8_t flags, uint16_t rsvd,
        uint32_t nsid, uint32_t cdw2, uint32_t cdw3, uint32_t cdw10, uint32_t cdw11,
        uint32_t cdw12, uint32_t cdw13, uint32_t cdw14, uint32_t cdw15,
        uint32_t data_len, void *data, uint32_t &result)
{
    struct nvme_passthru_cmd cmd = {
        .opcode		= opcode,
        .flags		= flags,
        .rsvd1		= rsvd,
        .nsid		= nsid,
        .cdw2		= cdw2,
        .cdw3		= cdw3,
        .metadata	= (uint64_t)(uintptr_t) NULL,
        .addr		= (uint64_t)(uintptr_t) data,
        .metadata_len	= 0,
        .data_len	= data_len,
        .cdw10		= cdw10,
        .cdw11		= cdw11,
        .cdw12		= cdw12,
        .cdw13		= cdw13,
        .cdw14		= cdw14,
        .cdw15		= cdw15,
        .timeout_ms	= 0,
        .result		= 0,
    };
    int err;
    /*{
        fprintf(stderr, "-- iLSM::DB::nvme_passthru --\n");
        fprintf(stderr, "opcode       : %02x\n", cmd.opcode);
        fprintf(stderr, "flags        : %02x\n", cmd.flags);
        fprintf(stderr, "rsvd1        : %04x\n", cmd.rsvd1);
        fprintf(stderr, "nsid         : %08x\n", cmd.nsid);
        fprintf(stderr, "cdw2         : %08x\n", cmd.cdw2);
        fprintf(stderr, "cdw3         : %08x\n", cmd.cdw3);
        fprintf(stderr, "data_len     : %08x\n", cmd.data_len);
        fprintf(stderr, "metadata_len : %08x\n", cmd.metadata_len);
        fprintf(stderr, "addr         : %llx\n", cmd.addr);
        fprintf(stderr, "metadata     : %llx\n", cmd.metadata);
        fprintf(stderr, "cdw10        : %08x\n", cmd.cdw10);
        fprintf(stderr, "cdw11        : %08x\n", cmd.cdw11);
        fprintf(stderr, "cdw12        : %08x\n", cmd.cdw12);
        fprintf(stderr, "cdw13        : %08x\n", cmd.cdw13);
        fprintf(stderr, "cdw14        : %08x\n", cmd.cdw14);
        fprintf(stderr, "cdw15        : %08x\n", cmd.cdw15);
        fprintf(stderr, "timeout_ms   : %08x\n", cmd.timeout_ms);
    }*/
    // err = ioctl(fd_, NVME_IOCTL_IO_CMD, &cmd);
    err = 0; // MOCK!!!!
    if (!err && result)
        result = cmd.result;
    return err;
}

