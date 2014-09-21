// the extent server implementation

#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <pthread.h>

#include "lang/verify.h"
#include "kv_server.h"

using namespace std;

kv_server::kv_server() 
{
	pthread_mutex_init(&lock, NULL);
}

/* The RPC reply argument "val" should contain 
 * the retrieved val together with its current version 
 */
int 
kv_server::get(std::string key, kv_protocol::versioned_val &val)
{
	// You fill this in for Lab 1.
	pthread_mutex_lock(&lock);
	if (kv_map.find(key)== kv_map.end() or kv_map.find(key)->second.deleteFlag){
		pthread_mutex_unlock(&lock);
		return kv_protocol::NOEXIST;
	}
	else{
		val.buf = kv_map.find(key)->second.buf;
		val.version = kv_map.find(key)->second.version;
	}
	pthread_mutex_unlock(&lock);
	return kv_protocol::OK;
}


/* the server should store the key-value entry, increment its version if appropriate.
   and put the new version of the stored entry in the RPC reply (i.e. the last argument)
*/
int 
kv_server::put(std::string key, std::string buf, int &new_version)
{
	// You fill this in for Lab 1.
	pthread_mutex_lock(&lock);
	if (kv_map.find(key) == kv_map.end()){
			value_pack v;
			v.buf = buf;
			v.version = 1;
			v.deleteFlag = false;
			new_version = 1;
			kv_map[key] = v;//insert v into the map
		}
		else{
			//set the delete flag
			kv_map.find(key)->second.deleteFlag = false;
			//set the value
			kv_map.find(key)->second.buf = buf;
			//set the version
			int ver = ((kv_map.find(key)->second.version)+=1);
			new_version = ver;
		}
	pthread_mutex_unlock(&lock);
	return kv_protocol::OK;
}

/* "remove" the existing key-value entry
 * do not actually delete it from your storage
 * treat it as a special type of put and 
 * increments the key-value pair's version number
 * like regular puts.
 * Set a delete flag to the "removed" key-value 
 * entry so a later get does not see it.
*/
int 
kv_server::remove(std::string key, int &new_version)
{
	// You fill this in for Lab 1.
	pthread_mutex_lock(&lock);
		if (kv_map.find(key) == kv_map.end()){
			value_pack v;
			v.buf = "empty";
			v.version = 1;
			v.deleteFlag = true;
			new_version = 1;
			kv_map[key] = v;
			pthread_mutex_unlock(&lock);
			return kv_protocol::NOEXIST;
		}
		else if(kv_map.find(key)->second.deleteFlag){
			int ver = ((kv_map.find(key)->second.version)+=1);
			//ver++;
			new_version = ver;
			//kv_map.find(key)->second.version = ver;
			pthread_mutex_unlock(&lock);
			return kv_protocol::NOEXIST;
		}
		else{
			kv_map.find(key)->second.deleteFlag = true;
			int ver = ((kv_map.find(key)->second.version)+=1);
			//ver++;
			new_version = ver;
			//kv_map.find(key)->second.version = ver;
		}
		pthread_mutex_unlock(&lock);
		return kv_protocol::OK;
}

int 
kv_server::stat(int x, std::string &msg)
{
	msg = "Server says: I am kicking";	
	return kv_protocol::OK;
}
