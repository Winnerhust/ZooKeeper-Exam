#include <iostream>
#include "inifile.h"
#include"zookeeper.h"  
#include"zookeeper_log.h"  


using namespace std;
using namespace inifile;

int g_reset = 0;
char g_host[512] = "172.17.0.36:2181";
char g_filepath[512] = "/Conf/test.ini";
/**********unitl*********************/  
void print_usage()
{
    printf("Usage : [testcase] [-h] [-m] [-s ip:port] \n");
    printf("        -h Show help\n");
    printf("        -p set the path of config data\n");
    printf("        -s server ip:port\n");
    printf("        -r retset data \n");
    printf("For example:\n");
    printf(">cat test.ini | testcase -r\n");
    printf("    put test.ini to zookeeper server\n");
    printf(">testcase -p/Conf/test.ini -s172.17.0.36:2181 \n");
    printf("    get config data from zookeeper server\n");
}
 
void get_option(int argc,const char* argv[])
{
	extern char    *optarg;
	int            optch;
	int            dem = 1;
	const char    optstring[] = "hrps:";
    
    
	while((optch = getopt(argc , (char * const *)argv , optstring)) != -1 )
	{
		switch( optch )
		{
		case 'h':
			print_usage();
			exit(-1);
		case '?':
			print_usage();
			printf("unknown parameter: %c\n", optopt);
			exit(-1);
		case ':':
			print_usage();
			printf("need parameter: %c\n", optopt);
			exit(-1);
        case 'r':
                g_reset = 1;
            break;
        case 's':
            strncpy(g_host,optarg,sizeof(g_host));
            break;
        case 'p':
            strncpy(g_filepath,optarg,sizeof(g_filepath));
            break;
		default:
			break;
		}
	}
} 
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
int main(int argc,const char *argv[])
{
    get_option(argc,argv);

    /**set data**/
    /** -r **/
    if(g_reset == 1){
        string s;
        string in_data;
        while(cin>>s){
            in_data += s + "\n";
        }
        cout<<"in_data:"<<in_data<<endl;
    
        setdata(g_host,g_filepath,in_data.c_str());
    }
   /** read test **/

    IniFile ini;
    ini.open2(g_host,g_filepath);

    //获取指定段的指定项的值
    int ret = 0;
    string db_name = ini.getStringValue("COMMON","DB",ret);
    string db_passwd = ini.getStringValue("COMMON","PASSWD",ret);
    
    cout<<"db_name:"<<db_name<<endl;;
    cout<<"db_passwd:"<<db_passwd<<endl;;
    return 0;
}
