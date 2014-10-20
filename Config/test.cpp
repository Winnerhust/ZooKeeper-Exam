#include <iostream>
#include "inifile.h"
#include"zookeeper.h"  
#include"zookeeper_log.h"  


using namespace std;
using namespace inifile;

int setdata(const char *host,const char * filepath,const char *data)
{

    int timeout = 30000;  
    char path_buffer[512];  
    int bufferlen=sizeof(path_buffer); 
    char conf_data[2048];
    int conf_len=sizeof(conf_data); 
  
    zoo_set_debug_level(ZOO_LOG_LEVEL_WARN); //设置日志级别,避免出现一些其他信息  

    zhandle_t* zkhandle = zookeeper_init(host,NULL, timeout, 0, (char *)"Config Test", 0);  

    if (zkhandle ==NULL)  
    {  
        fprintf(stderr, "Error when connecting to zookeeper servers...\n");  
        exit(EXIT_FAILURE);  
    }  
    int ret = zoo_exists(zkhandle,filepath,0,NULL);
    if(ret != ZOK){
        ret = zoo_create(zkhandle,filepath,data,strlen(data),NULL,0,path_buffer,bufferlen);
        if(ret != ZOK){
            fprintf(stderr, "Error when create path :%s\n",filepath);  
            exit(EXIT_FAILURE);  
        }
    }

    ret = zoo_set(zkhandle,filepath,data,strlen(data),-1);
    if(ret != ZOK){
        fprintf(stderr,"failed to set the data of path %s!\n",filepath);
    }
    
    zookeeper_close(zkhandle); 
}
int main(int argc,char *argv[])
{
    char host[512] = "172.17.0.36:2181";
    char filepath[512] = "/Conf/test.ini";
    
    if(argc >= 3){
        strncpy(host,argv[1],sizeof(host));
        strncpy(filepath,argv[2],sizeof(filepath));
    }
    /**set data**/
    char data[512]="[COMMON]\nDB=mysql\nPASSWD=root\n";

    setdata(host,filepath,data);

   /** read test **/

    IniFile ini;
    ini.open2(host,filepath);

    //获取指定段的指定项的值
    int ret = 0;
    string db_name = ini.getStringValue("COMMON","DB",ret);
    string db_passwd = ini.getStringValue("COMMON","PASSWD",ret);
    
    cout<<"db_name:"<<db_name<<endl;;
    cout<<"db_passwd:"<<db_passwd<<endl;;
    return 0;
}
