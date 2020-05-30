#pragma once

#ifdef __cplusplus
extern "C"{
#endif 

typedef unsigned char (*pg_recvlogical_on_changes_callback_f)(const void* context, const char* changes, unsigned int size);

int pg_recvlogical_init(int argc, char **argv);

void pg_recvlogical_stream_logical_start(const void* context, pg_recvlogical_on_changes_callback_f on_changes);

#ifdef __cplusplus
}
#endif