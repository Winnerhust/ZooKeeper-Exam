#include<stdio.h>  
#include<string.h>  
#include<unistd.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include"zookeeper.h"  
#include"zookeeper_log.h"  

enum MODE{PROVIDER_MODE,CONSUMER_MODE,MONITOR_MODE} g_mode;
char g_host[512]= "172.17.0.36:2181";  
char g_service[512]={ 0 };
char g_path[512]="/NameService";

//watch function when child list changed
void zktest_watcher_g(zhandle_t* zh, int type, int state, const char* path, void* watcherCtx);
//show all process ip:pid
void show_list(zhandle_t *zkhandle,const char *path);
//if success,the g_mode will become MODE_MONITOR
void choose_mater(zhandle_t *zkhandle,const char *path);
//get localhost ip:pid
void getlocalhost(char *ip_pid,int len);

void print_usage();
void get_option(int argc,const char* argv[]);

/**********unitl*********************/  
void print_usage()
{
    printf("Usage : [nameservice] [-h] [-m mode] [-n servicename] [-s ip:port] \n");
    printf("        -h Show help\n");
    printf("        -m set mode:provider,consumer,monitor\n");
    printf("        -n set servicename\n");
    printf("        -s server ip:port\n");
    printf("For example:\n");
    printf("    nameservice -m provider -n query_bill -s172.17.0.36:2181 \n");
    printf("    nameservice -m consumer -n query_bill -s172.17.0.36:2181 \n");
    printf("    nameservice -m monitor  -n query_bill -s172.17.0.36:2181 \n");
}
 
void get_option(int argc,const char* argv[])
{
	extern char    *optarg;
	int            optch;
	int            dem = 1;
	const char    optstring[] = "hm:n:s:";
    
    
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
        case 'm':
            if (strcasecmp(optarg,"provider") == 0){
                g_mode = PROVIDER_MODE;
            }else if (strcasecmp(optarg,"consumer") == 0){
                g_mode = CONSUMER_MODE;
            }else{
                g_mode = MONITOR_MODE;
            }
            break;
        case 'n':
            strncpy(g_service,optarg,sizeof(g_service));
            break;
        case 's':
            strncpy(g_host,optarg,sizeof(g_host));
            break;
		default:
			break;
		}
	}
} 
void zktest_watcher_g(zhandle_t* zh, int type, int state, const char* path, void* watcherCtx)  
{  
/*  
    printf("watcher event\n");  
    printf("type: %d\n", type);  
    printf("state: %d\n", state);  
    printf("path: %s\n", path);  
    printf("watcherCtx: %s\n", (char *)watcherCtx);  
*/  

    if(type == ZOO_CHILD_EVENT &&
       state == ZOO_CONNECTED_STATE &&
       g_mode == CONSUMER_MODE){
        
        printf("providers list changed!\n");
        show_list(zh,path);
    }else if(type == ZOO_CHILD_EVENT &&
             state == ZOO_CONNECTED_STATE &&
             g_mode == MONITOR_MODE){

        printf("providers or consumers list changed!\n");

        char child_path[512];
        printf("providers:\n");
        sprintf(child_path,"%s/%s/provider",g_path,g_service);
        show_list(zh,child_path);

        printf("consumers:\n");
        sprintf(child_path,"%s/%s/consumer",g_path,g_service);
        show_list(zh,child_path);
    }
}  
void getlocalhost(char *ip_pid,int len)
{
    char hostname[64] = {0};
    struct hostent *hent ;

    gethostname(hostname,sizeof(hostname));
    hent = gethostbyname(hostname);

    char * localhost = inet_ntoa(*((struct in_addr*)(hent->h_addr_list[0])));

    snprintf(ip_pid,len,"%s:%d",localhost,getpid());
}

void show_list(zhandle_t *zkhandle,const char *path)
{

    struct String_vector procs;
    int i = 0;
    char localhost[512]={0};

    getlocalhost(localhost,sizeof(localhost));

    int ret = zoo_get_children(zkhandle,path,1,&procs);
        
    if(ret != ZOK){
        fprintf(stderr,"failed to get the children of path %s!\n",path);
    }else{
        char child_path[512] ={0};
        char ip_pid[64] = {0};
        int ip_pid_len = sizeof(ip_pid);
        printf("--------------\n");
        printf("ip\tpid\n");
        for(i = 0; i < procs.count; ++i){
            sprintf(child_path,"%s/%s",path,procs.data[i]);
            //printf("%s\n",child_path);
            ret = zoo_get(zkhandle,child_path,0,ip_pid,&ip_pid_len,NULL);
            if(ret != ZOK){
                fprintf(stderr,"failed to get the data of path %s!\n",child_path);
            }else if(strcmp(ip_pid,localhost)==0){
                printf("%s(Master)\n",ip_pid);
            }else{
                printf("%s\n",ip_pid);
            }
        }
    }

    for(i = 0; i < procs.count; ++i){
        free(procs.data[i]);
        procs.data[i] = NULL;
    }
}
int create(zhandle_t *zkhandle,const char *path,const char *ctx,int flag)
{
    char path_buffer[512];  
    int bufferlen=sizeof(path_buffer);  

    int ret = zoo_exists(zkhandle,path,0,NULL); 
    if(ret != ZOK){
        ret = zoo_create(zkhandle,path,ctx,strlen(ctx),  
                          &ZOO_OPEN_ACL_UNSAFE,flag,  
                          path_buffer,bufferlen);  
        if(ret != ZOK){
            fprintf(stderr,"failed to create the path %s!\n",path);
        }else{
            printf("create path %s successfully!\n",path);
        }
    }

    return ZOK;
}

int main(int argc, const char *argv[])  
{  
    int timeout = 30000;  
    char path_buffer[512];  
    int bufferlen=sizeof(path_buffer);  
    int ret = 0;
    zoo_set_debug_level(ZOO_LOG_LEVEL_ERROR); //设置日志级别,避免出现一些其他信息  

    get_option(argc,argv);

    zhandle_t* zkhandle = zookeeper_init(g_host,zktest_watcher_g, timeout, 0, (char *)"NameService Test", 0);  

    if (zkhandle ==NULL)  
    {  
        fprintf(stderr, "Error when connecting to zookeeper servers...\n");  
        exit(EXIT_FAILURE);  
    }  
    
    create(zkhandle,g_path,"NameService Test",0);

    sprintf(path_buffer,"%s/%s",g_path,g_service);
    create(zkhandle,path_buffer,"NameService Test",0);
    
    sprintf(path_buffer,"%s/%s/provider",g_path,g_service);
    create(zkhandle,path_buffer,"NameService Test",0);
    
    sprintf(path_buffer,"%s/%s/consumer",g_path,g_service);
    create(zkhandle,path_buffer,"NameService Test",0);
  
    if(g_mode == PROVIDER_MODE){
        
        char localhost[512]={0};
        getlocalhost(localhost,sizeof(localhost));
        
        char child_path[512];
        sprintf(child_path,"%s/%s/provider/",g_path,g_service);
        ret = zoo_create(zkhandle,child_path,localhost,strlen(localhost),  
                          &ZOO_OPEN_ACL_UNSAFE,ZOO_SEQUENCE|ZOO_EPHEMERAL,  
                          path_buffer,bufferlen);  
        if(ret != ZOK){
            fprintf(stderr,"failed to create the child_path %s,buffer:%s!\n",child_path,path_buffer);
        }else{
            printf("create child path %s successfully!\n",path_buffer);
        }

    }else if (g_mode == CONSUMER_MODE){
        
        char localhost[512]={0};
        getlocalhost(localhost,sizeof(localhost));
        
        char child_path[512];
        sprintf(child_path,"%s/%s/consumer/",g_path,g_service);
        ret = zoo_create(zkhandle,child_path,localhost,strlen(localhost),  
                          &ZOO_OPEN_ACL_UNSAFE,ZOO_SEQUENCE|ZOO_EPHEMERAL,  
                          path_buffer,bufferlen);  
        if(ret != ZOK){
            fprintf(stderr,"failed to create the child_path %s,buffer:%s!\n",child_path,path_buffer);
        }else{
            printf("create child path %s successfully!\n",path_buffer);
        }
        
        sprintf(child_path,"%s/%s/provider",g_path,g_service);
        show_list(zkhandle,child_path);

    }else if(g_mode == MONITOR_MODE){
        char child_path[512];
        printf("providers:\n");
        sprintf(child_path,"%s/%s/provider",g_path,g_service);
        show_list(zkhandle,child_path);

        printf("consumers:\n");
        sprintf(child_path,"%s/%s/consumer",g_path,g_service);
        show_list(zkhandle,child_path);
    }
    
    getchar();
    
    zookeeper_close(zkhandle); 

    return 0;
} 
