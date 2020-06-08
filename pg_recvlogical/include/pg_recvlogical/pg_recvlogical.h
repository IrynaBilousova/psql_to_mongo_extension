#ifndef __pg_recvlogical_h__
#define __pg_recvlogical_h__

typedef unsigned char (*pg_recvlogical_on_changes_callback_f)(const void* context, const char* changes, unsigned int size);

struct pg_recvlogical_connection_settings_t
{
    const char* _dbname;
	const char* _host;
	const char* _port;
	const char* _username;
	const char* _password;
    unsigned int _id;
};

struct pg_recvlogical_replication_settings_t
{
	const char* _startpos;
	const char* _endpos;
	const char* _option;
	const char* _plugin;
	unsigned    _status_interval;/* 10 * 1000;	10 sec = default */
	const char* _slot;
} ;

struct pg_recvlogical_init_settings_t
{
    struct pg_recvlogical_connection_settings_t _connection;
    struct pg_recvlogical_replication_settings_t _repication;
    char _verbose;
};

#ifdef __cplusplus
extern "C"{
#endif 

int pg_recvlogical_init(const struct pg_recvlogical_init_settings_t* pg_recvlogical_settings, const char* exec_path);

void pg_recvlogical_stream_logical_start(const void* context, pg_recvlogical_on_changes_callback_f on_changes);

#ifdef __cplusplus
}
#endif

#endif