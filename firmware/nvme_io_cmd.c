//////////////////////////////////////////////////////////////////////////////////
// nvme_io_cmd.c for Cosmos+ OpenSSD
// Copyright (c) 2016 Hanyang University ENC Lab.
// Contributed by Yong Ho Song <yhsong@enc.hanyang.ac.kr>
//				  Youngjin Jo <yjjo@enc.hanyang.ac.kr>
//				  Sangjin Lee <sjlee@enc.hanyang.ac.kr>
//				  Jaewook Kwak <jwkwak@enc.hanyang.ac.kr>
//
// This file is part of Cosmos+ OpenSSD.
//
// Cosmos+ OpenSSD is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 3, or (at your option)
// any later version.
//
// Cosmos+ OpenSSD is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
// See the GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with Cosmos+ OpenSSD; see the file COPYING.
// If not, see <http://www.gnu.org/licenses/>.
//////////////////////////////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////////////////////////////
// Company: ENC Lab. <http://enc.hanyang.ac.kr>
// Engineer: Sangjin Lee <sjlee@enc.hanyang.ac.kr>
//			 Jaewook Kwak <jwkwak@enc.hanyang.ac.kr>
//
// Project Name: Cosmos+ OpenSSD
// Design Name: Cosmos+ Firmware
// Module Name: NVMe IO Command Handler
// File Name: nvme_io_cmd.c
//
// Version: v1.0.1
//
// Description:
//   - handles NVMe IO command
//////////////////////////////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////////////////////////////
// Revision History:
//
// * v1.0.1
//   - header file for buffer is changed from "ia_lru_buffer.h" to "lru_buffer.h"
//
// * v1.0.0
//   - First draft
//////////////////////////////////////////////////////////////////////////////////


#include "xil_printf.h"
#include "debug.h"
#include "io_access.h"

#include "nvme.h"
#include "host_lld.h"
#include "nvme_io_cmd.h"
#include "../memory_map.h"

#include "../ftl_config.h"
#include "../request_transform.h"
#include "../sstable/sstable.h"
#include "../sstable/super.h"
#include "../memtable/memtable.h"
#include "../iterator/iterator.h"
#include "../data_buffer.h"
#include "xtime_l.h"

#define SEED1 0xcc9ed51
#define HASH_NUM 30

//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////// BandSlim ////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
// * Turn ON/OFF debugging prints 
#define BANDSLIM_DEBUG

// * Turn ON/OFF NAND flash I/Os
// #define NAND_IO_DISABLE   

// * If NAND I/O is enabled, check out if it's adaptive-combi transfer case
#ifndef NAND_IO_DISABLE
    #define ADAPT_COMBI
#endif

// Allocate NAND page buffer entry and evict (NAND write) if the buffer is full
unsigned int get_nand_page_buffer_entry(const unsigned int logicalSliceAddr) {
    unsigned int dataBufEntry, dataBufAddr; int i;
    dataBufEntry = CheckDataBufHitWithLSA(logicalSliceAddr);

    if (dataBufEntry == DATA_BUF_FAIL) {
        dataBufEntry = AllocateDataBuf();
        EvictDataBufEntryForMemoryCopy(dataBufEntry);
        dataBufMapPtr->dataBuf[dataBufEntry].logicalSliceAddr = logicalSliceAddr;
        PutToDataBufHashList(dataBufEntry);
        SyncAllLowLevelReqDone();
    }

    dataBufMapPtr->dataBuf[dataBufEntry].dirty = DATA_BUF_DIRTY;

    dataBufAddr = (DATA_BUFFER_BASE_ADDR + dataBufEntry * BYTES_PER_DATA_REGION_OF_SLICE);
    return dataBufAddr;
}

/* Get the closest 4KB-aligned address inside the buffer entry */
unsigned int get_mem_page_boundary(unsigned int offset) {
    unsigned int boundaries[] = {0, 4096, 8192, 12288, 16384}; int i;

    for (i = 0; i < 5; i++) {
        if (boundaries[i] >= offset)
	    return boundaries[i];
    }

    return boundaries[4];
}

/* Custom NAND page buffer management for BandSlim */
uint8_t *vlogblock[VLOGBLOCK_NUMBER];           // Addr pointer for NAND page buffer entries
unsigned int vlogblock_left[VLOGBLOCK_NUMBER];  // Left bytes of each NAND page buffer entry
unsigned int vlogblock_turn;                    // Currently turned-on block
unsigned int vlog_offset;                       // Current offset of current value_log_lba
unsigned int vlog_value_length;                 // Value size of current time (single threaded)

/* Initialize the custom NAND page buffer for BandSlim */
void vlogblock_init(void) {
    vlogblock_turn = 0;

    vlogblock[vlogblock_turn] = (uint8_t*)get_nand_page_buffer_entry(value_log_lba / NVME_BLOCKS_PER_SLICE);
    vlogblock_left[vlogblock_turn] = BYTES_PER_DATA_REGION_OF_SLICE;
    
    vlog_offset = 0;
}

/* Issue PRP-based DMA transactions to current NAND page buffer entry */
int vlogblock_issue_rx_dma(unsigned int cmdSlotTag, NVME_IO_COMMAND *nvmeIOCmd, unsigned int *kv_lba, unsigned int *kv_index) {
    int ret = 1, no_combi_flag = 0; unsigned int start_offset, end_offset;
    unsigned int buf_addr, dma_offset, total_dma_size, total_nvme_block, num_nvme_block = 0;

#ifndef ADAPT_COMBI
    total_dma_size = (((vlog_value_length - 1) / BYTES_PER_NVME_BLOCK) + 1) * BYTES_PER_NVME_BLOCK;
#else
    total_dma_size = ((vlog_value_length - 1) / BYTES_PER_NVME_BLOCK) * BYTES_PER_NVME_BLOCK;
    if (total_dma_size < BYTES_PER_NVME_BLOCK) {
        total_dma_size = BYTES_PER_NVME_BLOCK;
        no_combi_flag = 1;
    }
#endif
    total_nvme_block = total_dma_size / BYTES_PER_NVME_BLOCK; 
    start_offset = vlog_offset; 
    dma_offset = get_mem_page_boundary(vlog_offset);

    while (num_nvme_block < total_nvme_block) {
        if (dma_offset == BYTES_PER_DATA_REGION_OF_SLICE) {
#ifndef NAND_IO_DISABLE
            vlogblock_flush();
#endif
            dma_offset = 0;
        }

        // Get the target address	
        buf_addr = (unsigned int)vlogblock[vlogblock_turn] + dma_offset; 
        if (num_nvme_block == 0) {
            *kv_lba = value_log_lba;
            *kv_index = dma_offset;
        }

        // Construct NVMe RxDMA for each 4KB
        set_auto_rx_dma(cmdSlotTag, 0, buf_addr, NVME_COMMAND_AUTO_COMPLETION_OFF);
        num_nvme_block++;
#ifdef BANDSLIM_DEBUG
        xil_printf("turn: %u, buf_addr: %u, ofs:%u, dma_ofs: %u, left: %u, dma_cnt:%u\r\n", vlogblock_turn, (unsigned int)vlogblock[vlogblock_turn], vlog_offset, dma_offset, vlogblock_left[vlogblock_turn], num_nvme_block);
#endif
        if (num_nvme_block != total_nvme_block)	
            dma_offset = get_mem_page_boundary(dma_offset + 1);
    }
    // Issue RxDMA transactions
    check_auto_rx_dma_done();

#ifndef ADAPT_COMBI
    vlog_offset = dma_offset + vlog_value_length;
    vlog_value_length = 0;
#else
    if ((total_dma_size == BYTES_PER_NVME_BLOCK) && no_combi_flag) {
        vlog_offset = dma_offset + vlog_value_length;
        vlog_value_length = 0;
    }
    else { 
        vlog_offset = dma_offset + total_dma_size;
        vlog_value_length -= total_dma_size;
    }
#endif
    end_offset = vlog_offset;
    vlogblock_left[vlogblock_turn] -= (end_offset - start_offset);

    return ret;
}

// * Macro function for checking value size
#define IS_LEFT(left) left > 0 && left <= BYTES_PER_DATA_REGION_OF_SLICE
// * Macro function for inserting piggybacked value (be sure to wrap up this macro with {,})
#define PIGGYBACK_VALUE(vlog, cdw, left, step, ofs) \
        memcpy((uint8_t*)(vlog + ofs), &(cdw), (left < step ? left : step)); \
        ofs += (left < step ? left : step); \
        left -= step; 

/* Copy piggybacked values to the current Value Log offset (write command) */
// Known limitation of Cosmos+ OpenSSD during fine-grained value packing
//  - the platform cannot process memcpy operation on non-word-aligned target addrs
//  - thus the user always has to put word-aligned-sized values to the device
int vlogblock_insert(NVME_IO_COMMAND *nvmeIOCmd, unsigned int *kv_lba, unsigned int *kv_index) {
    int ret = 1; unsigned int start_offset, end_offset;

    if (vlogblock_left[vlogblock_turn] >= vlog_value_length && 
        vlogblock_left[vlogblock_turn] <= BYTES_PER_DATA_REGION_OF_SLICE) {
        start_offset = vlog_offset;
        
        *kv_lba = value_log_lba;
        *kv_index = start_offset;
        
        if (IS_LEFT(vlog_value_length)) { PIGGYBACK_VALUE(vlogblock[vlogblock_turn], nvmeIOCmd->dword[4], vlog_value_length, 4, vlog_offset) }
        if (IS_LEFT(vlog_value_length)) { PIGGYBACK_VALUE(vlogblock[vlogblock_turn], nvmeIOCmd->dword[5], vlog_value_length, 4, vlog_offset) }
        if (IS_LEFT(vlog_value_length)) { PIGGYBACK_VALUE(vlogblock[vlogblock_turn], nvmeIOCmd->dword[6], vlog_value_length, 4, vlog_offset) }
        if (IS_LEFT(vlog_value_length)) { PIGGYBACK_VALUE(vlogblock[vlogblock_turn], nvmeIOCmd->dword[7], vlog_value_length, 4, vlog_offset) }
        if (IS_LEFT(vlog_value_length)) { PIGGYBACK_VALUE(vlogblock[vlogblock_turn], nvmeIOCmd->dword[8], vlog_value_length, 4, vlog_offset) }
        if (IS_LEFT(vlog_value_length)) { PIGGYBACK_VALUE(vlogblock[vlogblock_turn], nvmeIOCmd->dword[9], vlog_value_length, 4, vlog_offset) }
        if (IS_LEFT(vlog_value_length)) { PIGGYBACK_VALUE(vlogblock[vlogblock_turn], nvmeIOCmd->dword[11], vlog_value_length, 4, vlog_offset) }
        if (IS_LEFT(vlog_value_length)) { PIGGYBACK_VALUE(vlogblock[vlogblock_turn], nvmeIOCmd->dword[12], vlog_value_length, 4, vlog_offset) }
        if (IS_LEFT(vlog_value_length)) { PIGGYBACK_VALUE(vlogblock[vlogblock_turn], nvmeIOCmd->dword[13], vlog_value_length, 4, vlog_offset) }

        end_offset = vlog_offset;
        vlogblock_left[vlogblock_turn] -= (end_offset - start_offset);
    }
    else {
        vlogblock_flush();
        ret = 0;      // Insert again!
    }
#ifdef BANDSLIM_DEBUG
    xil_printf("turn: %u, buf_addr: %u, ofs: %u, left: %u\r\n", vlogblock_turn, (unsigned int)vlogblock[vlogblock_turn], vlog_offset, vlogblock_left[vlogblock_turn]);
#endif
    return ret;
}

/* Copy piggybacked values to the current Value Log offset (transfer command) */
int vlogblock_append(NVME_IO_COMMAND *nvmeIOCmd) {
    int ret = 1; unsigned int start_offset, end_offset;

    if (vlogblock_left[vlogblock_turn] >= vlog_value_length &&
        vlogblock_left[vlogblock_turn] <= BYTES_PER_DATA_REGION_OF_SLICE) {
        start_offset = vlog_offset;

        if (IS_LEFT(vlog_value_length)) { PIGGYBACK_VALUE(vlogblock[vlogblock_turn], nvmeIOCmd->dword[2], vlog_value_length, 4, vlog_offset) }
        if (IS_LEFT(vlog_value_length)) { PIGGYBACK_VALUE(vlogblock[vlogblock_turn], nvmeIOCmd->dword[3], vlog_value_length, 4, vlog_offset) }
        if (IS_LEFT(vlog_value_length)) { PIGGYBACK_VALUE(vlogblock[vlogblock_turn], nvmeIOCmd->dword[4], vlog_value_length, 4, vlog_offset) }
        if (IS_LEFT(vlog_value_length)) { PIGGYBACK_VALUE(vlogblock[vlogblock_turn], nvmeIOCmd->dword[5], vlog_value_length, 4, vlog_offset) }
        if (IS_LEFT(vlog_value_length)) { PIGGYBACK_VALUE(vlogblock[vlogblock_turn], nvmeIOCmd->dword[6], vlog_value_length, 4, vlog_offset) }
        if (IS_LEFT(vlog_value_length)) { PIGGYBACK_VALUE(vlogblock[vlogblock_turn], nvmeIOCmd->dword[7], vlog_value_length, 4, vlog_offset) }
        if (IS_LEFT(vlog_value_length)) { PIGGYBACK_VALUE(vlogblock[vlogblock_turn], nvmeIOCmd->dword[8], vlog_value_length, 4, vlog_offset) }
        if (IS_LEFT(vlog_value_length)) { PIGGYBACK_VALUE(vlogblock[vlogblock_turn], nvmeIOCmd->dword[9], vlog_value_length, 4, vlog_offset) }
        if (IS_LEFT(vlog_value_length)) { PIGGYBACK_VALUE(vlogblock[vlogblock_turn], nvmeIOCmd->dword[10], vlog_value_length, 4, vlog_offset) }
        if (IS_LEFT(vlog_value_length)) { PIGGYBACK_VALUE(vlogblock[vlogblock_turn], nvmeIOCmd->dword[11], vlog_value_length, 4, vlog_offset) }
        if (IS_LEFT(vlog_value_length)) { PIGGYBACK_VALUE(vlogblock[vlogblock_turn], nvmeIOCmd->dword[12], vlog_value_length, 4, vlog_offset) }
        if (IS_LEFT(vlog_value_length)) { PIGGYBACK_VALUE(vlogblock[vlogblock_turn], nvmeIOCmd->dword[13], vlog_value_length, 4, vlog_offset) }
        if (IS_LEFT(vlog_value_length)) { PIGGYBACK_VALUE(vlogblock[vlogblock_turn], nvmeIOCmd->dword[14], vlog_value_length, 4, vlog_offset) }
        if (IS_LEFT(vlog_value_length)) { PIGGYBACK_VALUE(vlogblock[vlogblock_turn], nvmeIOCmd->dword[15], vlog_value_length, 4, vlog_offset) }
    
        end_offset = vlog_offset;
        vlogblock_left[vlogblock_turn] -= (end_offset - start_offset);
    }
    else {
        vlogblock_flush();
        ret = 0;      // Insert again!
    }
#ifdef BANDSLIM_DEBUG
    xil_printf("turn: %u, buf_addr: %u, ofs: %u, left: %u\r\n", vlogblock_turn, (unsigned int)vlogblock[vlogblock_turn], vlog_offset, vlogblock_left[vlogblock_turn]);
#endif
    return ret;
}

/* Allocate new NAND page buffer entry and evict the oldest allocated one (if buffer is full) */
void vlogblock_flush(void) {
    if (++vlogblock_turn >= VLOGBLOCK_NUMBER) 
        vlogblock_turn = 0;

    value_log_lba += NVME_BLOCKS_PER_PAGE;
    vlogblock[vlogblock_turn] = (uint8_t*)get_nand_page_buffer_entry(value_log_lba / NVME_BLOCKS_PER_SLICE);
    
    vlogblock_left[vlogblock_turn] = BYTES_PER_DATA_REGION_OF_SLICE;
    vlog_offset = 0;
}

// PRP-based DMA
void handle_nvme_io_kv_put(unsigned int cmdSlotTag, NVME_IO_COMMAND *nvmeIOCmd)
{
    IO_READ_COMMAND_DW12 writeInfo12;
    unsigned int startLba[2], nlb, kv_key, kv_length, kv_nlb, kv_lba, kv_index;
     
    writeInfo12.dword = nvmeIOCmd->dword[12];
    if(writeInfo12.FUA == 1) xil_printf("write FUA\r\n");
    nlb = writeInfo12.NLB;

    kv_key = nvmeIOCmd->dword[2];       // CDW2 -> Key
    kv_length = nvmeIOCmd->dword[10];   // CDW10 -> Value Size
    vlog_value_length = kv_length;      // Global value size (single threaded machine)

    kv_nlb = nlb + 1;                   // # of pages needed
    ASSERT(kv_nlb == (kv_length / BYTES_PER_SECTOR) + ((kv_length % BYTES_PER_SECTOR) > 0 ? 1 : 0));
#ifdef BANDSLIM_DEBUG
    xil_printf("BandSlim PRP Write Command\r\n");
#endif
    // Insert (issue page-unit DMA) to the Value Log
    while (vlogblock_issue_rx_dma(cmdSlotTag, nvmeIOCmd, kv_lba, kv_index) == 0);
#ifndef NAND_IO_DISABLE       	
    /*** Implement the LSM-tree insertion routine here ***/
#endif
    NVME_COMPLETION nvmeCPL;
    nvmeCPL.dword[0] = 0;
    nvmeCPL.specific = kv_length;
    set_auto_nvme_cpl(cmdSlotTag, nvmeCPL.specific, nvmeCPL.statusFieldWord);
}

// Write Command
void handle_nvme_io_bandslim_write(unsigned int cmdSlotTag, NVME_IO_COMMAND *nvmeIOCmd)
{
    unsigned int kv_key, kv_length, kv_lba, kv_index;

    kv_key = nvmeIOCmd->dword[2];       // CDW2 -> Key
    kv_length = nvmeIOCmd->dword[10];   // CDW10 -> Value Size
    vlog_value_length = kv_length;      // Global value size (single threaded machine)

#ifdef BANDSLIM_DEBUG
    xil_printf("BandSlim Write Command\r\n");
    xil_printf("%x%x ", nvmeIOCmd->dword[4], nvmeIOCmd->dword[5]);
    xil_printf("%x%x ", nvmeIOCmd->dword[6], nvmeIOCmd->dword[7]);
    xil_printf("%x ", nvmeIOCmd->dword[8]); xil_printf("%x ", nvmeIOCmd->dword[9]);
    xil_printf("%x ", nvmeIOCmd->dword[11]); xil_printf("%x ", nvmeIOCmd->dword[12]);
    xil_printf("%x\r\n", nvmeIOCmd->dword[13]); 
#endif
#ifndef NAND_IO_DISABLE       	
    // Insert to the Value Log
    while (vlogblock_insert(nvmeIOCmd, &kv_lba, &kv_index) == 0);

    /*** Implement the LSM-tree insertion routine here ***/
#endif
    NVME_COMPLETION nvmeCPL;
    nvmeCPL.dword[0] = 0;
    nvmeCPL.specific = kv_length;
    set_auto_nvme_cpl(cmdSlotTag, nvmeCPL.specific, nvmeCPL.statusFieldWord);
}

// Transfer Command
void handle_nvme_io_bandslim_transfer(unsigned int cmdSlotTag, NVME_IO_COMMAND *nvmeIOCmd)
{
#ifdef BANDSLIM_DEBUG
    xil_printf("BandSlim Transfer Command\r\n");
    xil_printf("%x ", nvmeIOCmd->dword[2]); xil_printf("%x ", nvmeIOCmd->dword[3]);
    xil_printf("%x%x ", nvmeIOCmd->dword[4], nvmeIOCmd->dword[5]);
    xil_printf("%x%x ", nvmeIOCmd->dword[6], nvmeIOCmd->dword[7]);
    xil_printf("%x ", nvmeIOCmd->dword[8]); xil_printf("%x ", nvmeIOCmd->dword[9]);
    xil_printf("%x ", nvmeIOCmd->dword[10]); xil_printf("%x ", nvmeIOCmd->dword[11]);
    xil_printf("%x ", nvmeIOCmd->dword[11]); xil_printf("%x ", nvmeIOCmd->dword[12]);
    xil_printf("%x ", nvmeIOCmd->dword[13]); xil_printf("%x ", nvmeIOCmd->dword[14]);
    xil_printf("%x\r\n", nvmeIOCmd->dword[15]); 
#endif    
#ifndef NAND_IO_DISABLE       	
    while (vlogblock_append(nvmeIOCmd) == 0);
#endif
    NVME_COMPLETION nvmeCPL;
    nvmeCPL.dword[0] = 0;
    nvmeCPL.specific = 0;
    set_auto_nvme_cpl(cmdSlotTag, nvmeCPL.specific, nvmeCPL.statusFieldWord);
}

void handle_nvme_io_cmd(NVME_COMMAND *nvmeCmd)
{
    NVME_IO_COMMAND *nvmeIOCmd;
    NVME_COMPLETION nvmeCPL;
    nvmeIOCmd = (NVME_IO_COMMAND*)nvmeCmd->cmdDword;
    unsigned int opc = (unsigned int)nvmeIOCmd->OPC;

    switch(opc)
    {
        case IO_NVM_KV_PUT:
        {
            handle_nvme_io_kv_put(nvmeCmd->cmdSlotTag, nvmeIOCmd);
            /*** Implement the LSM-tree compaction routine here ***/
            break;
        }
        case IO_NVM_KV_BANDSLIM_WRITE:
        {
            handle_nvme_io_bandslim_write(nvmeCmd->cmdSlotTag, nvmeIOCmd);
            break;
        }
        case IO_NVM_KV_BANDSLIM_TRANSFER:
        {
            handle_nvme_io_bandslim_transfer(nvmeCmd->cmdSlotTag, nvmeIOCmd);
            break;
        }
        default:
        {
            xil_printf("Not Support IO Command OPC: %X\r\n", opc);
            ASSERT(0);
            break;
        }
    }
}

//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////// BandSlim ////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
