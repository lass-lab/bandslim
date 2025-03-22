#pragma once

#include <string>
#include <mutex>
#include <chrono>
#include <vector>

#define MAX_ITER_NUM 100

#define THREAD_SAFE_ILSM
#define GET_FOR_SEEK_AND_NEXT_ILSM // BandSlim

namespace iLSM {
    class DB{
        public:
            DB() : fd_(-1) {
                op_stat.t.resize(6);
                op_stat.c.resize(6);
                passthru_stat.t.resize(8);   // BandSlim
                passthru_stat.c.resize(8, 0);// BandSlim
            }
            int Open(const std::string &dev);
            int Put(const std::string &key, const std::string &value);
            int Get(const std::string &key, std::string &value);
            int CreateIter(unsigned int &iter_id);
            int Seek(const unsigned int iter_id, const std::string &key, std::string &value);
            int Next(const unsigned int iter_id, std::string &value);
            int DestroyIter(const unsigned int iter_id);

            std::string Report();
        private:
            enum NvmeOpcode {
                NVME_CMD_KV_PUT                 = 0xA0,
                NVME_CMD_KV_GET                 = 0xA1,
                NVME_CMD_KV_DELETE              = 0xA2,
                NVME_CMD_KV_ITER_CREATE_ITER    = 0xA3,
                NVME_CMD_KV_ITER_SEEK           = 0xA4,
                NVME_CMD_KV_ITER_NEXT           = 0xA5,
                NVME_CMD_KV_ITER_DESTROY_ITER   = 0xA6,
                ////////////////////////////////////////////////////////////////
                /////////////////////////// BandSlim ///////////////////////////
                ////////////////////////////////////////////////////////////////
                NVME_CMD_KV_LAST                = 0xA8,  
                NVME_CMD_KV_BANDSLIM_WRITE        = 0xA7,   
                NVME_CMD_KV_BANDSLIM_TRANSFER     = 0xA9,   
                ////////////////////////////////////////////////////////////////
                /////////////////////////// BandSlim ///////////////////////////
                ////////////////////////////////////////////////////////////////
            };
            
            enum class iLSMOp :int{
                Put             = 0,
                Get             = 1,
                CreateIter      = 2,
                Seek            = 3,
                Next            = 4,
                DestroyIter     = 5,
                LAST            = 6,
            };

            struct OP_STAT {
                std::vector<std::chrono::nanoseconds> t;
                std::vector<int> c;
            } op_stat;

            struct PASSTHRU_STAT {
                std::vector<std::chrono::nanoseconds> t;
                std::vector<int> c;
            } passthru_stat;

            int fd_;
            int cnt=0;
#ifdef THREAD_SAFE_ILSM
            std::mutex ioctl_mtx;
            std::mutex op_stat_mtx;
            std::mutex passthru_stat_mtx;
            std::mutex report_mtx;
#endif

            inline int _Put(const std::string &key, const std::string &value);
            inline int _Get(const std::string &key, std::string &value);
            inline int _CreateIter(unsigned int &iter_id);
            inline int _Seek(const unsigned int iter_id, const std::string &key, std::string &value);
            inline int _Next(const unsigned int iter_id, std::string &value);
            inline int _DestroyIter(const unsigned int iter_id);
            int nvme_passthru(uint8_t opcode,
                    uint8_t flags, uint16_t rsvd, uint32_t nsid,
                    uint32_t cdw2, uint32_t cdw3, uint32_t cdw10, uint32_t cdw11,
                    uint32_t cdw12, uint32_t cdw13, uint32_t cdw14, uint32_t cdw15,
                    uint32_t data_len, void *data, uint32_t &result);
            ////////////////////////////////////////////////////////////////
            /////////////////////////// BandSlim ///////////////////////////
            ////////////////////////////////////////////////////////////////
            int nvme_passthru_bandslim(
                    uint8_t opcode, uint8_t flags, uint16_t rsvd, uint32_t nsid, 
                    uint32_t cdw2, uint32_t cdw3, uint64_t cdw4_5, uint64_t cdw6_7, 
	            uint32_t cdw8, uint32_t cdw9, uint32_t cdw10, uint32_t cdw11,
                    uint32_t cdw12, uint32_t cdw13, uint32_t cdw14, uint32_t cdw15,
                    uint32_t &result);
            ////////////////////////////////////////////////////////////////
            /////////////////////////// BandSlim ///////////////////////////
            ////////////////////////////////////////////////////////////////

            void finishOp(const enum iLSMOp op, std::chrono::nanoseconds &d);
            void finishPassthru(const enum NvmeOpcode opcode, std::chrono::nanoseconds &d);

#ifdef GET_FOR_SEEK_AND_NEXT_ILSM
            // For Iterator Using Get()
            struct _iterator {
                unsigned int key;
            };
            
            struct _iterator iter[MAX_ITER_NUM];
            unsigned long long numGetofSeek=0;
            unsigned long long numGetofNext=0;
#endif
    };
}
