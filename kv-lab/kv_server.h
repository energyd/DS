// this is the extent server

#ifndef kv_server_h
#define kv_server_h

#include <string>
#include <map>
#include "kv_protocol.h"


class kv_server {


	public:
		kv_server();

		int put(std::string key, std::string buf, int &new_version);
		int get(std::string key, kv_protocol::versioned_val &val);
		int remove(std::string key, int &);
		int stat(int, std::string &msg);
		typedef struct {
					  int version;
					  std::string buf;
					  bool deleteFlag;
				  } value_pack;

	private:
		pthread_mutex_t lock;
		std::map<std::string, value_pack> kv_map;
};

#endif 


