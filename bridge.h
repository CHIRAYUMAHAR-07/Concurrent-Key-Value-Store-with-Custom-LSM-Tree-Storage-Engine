#pragma once
#ifndef BRIDGE_H
#define BRIDGE_H
#include <stdint.h>
#include <stdlib.h>
#ifdef __cplusplus
extern "C"
{
#endif

    typedef void *LSMHandle;

    LSMHandle lsm_create(const char *data_dir);
    void lsm_destroy(LSMHandle h);
    int lsm_put(LSMHandle h, const char *key, const char *val, int vlen);
    char *lsm_get(LSMHandle h, const char *key, int *out_len);
    char *lsm_scan(LSMHandle h, const char *start, const char *end, int limit, int *out_count);

    char *lsm_stats(LSMHandle h);

    void lsm_free(void *p);
    void lsm_flush(LSMHandle h);
    int lsm_sst_count(LSMHandle h);

#ifdef __cplusplus
}
#endif
#endif
