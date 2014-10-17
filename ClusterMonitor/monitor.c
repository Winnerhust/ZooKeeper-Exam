#include<stdio.h>  
#include<string.h>  
#include<unistd.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include"zookeeper.h"  
#include"zookeeper_log.h"  

enum WORK_MODE{MODE_MONITOR,MODE_WORKER} g_mode;
char g_host[512]= "172.17.0.36:2181";  

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
    printf("Usage : [monitor] [-h] [-m] [-s ip:port] \n");
    printf("        -h Show help\n");
    printf("        -m set monitor mode\n");
    printf("        -s server ip:port\n");
    printf("For example:\n");
    printf("monitor -m -s172.17.0.36:2181 \n");
}
 
void get_option(int argc,const char* argv[])
{
	extern char    *optarg;
	int            optch;
	int            dem = 1;
	const char    optstring[] = "hms:";
    
    //default    
    g_mode = MODE_WORKER;
    
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
                g_mode = MODE_MONITOR;
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
       state == ZOO_CONNECTED_STATE ){

        choose_mater(zh,path);
        if(g_mode == MODE_MONITOR){
            show_list(zh,path);
        }
    }
}  
void getlocalhost(char *ip_pid,int len)
{
    char hostname[64] = {0};
    struct hostent *hent ;

    gethostname(hostname,sizeof(hostname));
    hent = gethostbyname(hostname);

    char * localhost = inet_ntoa(*((struct in_addr*)(hent->h_addr_list[0])));

    snprintf(ip_pid,len,"%s:%lld",localhost,getpid());
}

void choose_mater(zhandle_t *zkhandle,const char *path)
{
    struct String_vector procs;
    int i = 0;
    int ret = zoo_get_children(zkhandle,path,1,&procs);
        
    if(ret != ZOK || procs.count == 0){
        fprintf(stderr,"failed to get the children of path %s!\n",path);
    }else{
        char master_path[512] ={0};
        char ip_pid[64] = {0};
        int ip_pid_len = sizeof(ip_pid);
        
        char master[512]={0};
        char localhost[512]={0};
        
        getlocalhost(localhost,sizeof(localhost));
        
        strcpy(master,procs.data[0]);
        for(i = 1; i < procs.count; ++i){
            if(strcmp(master,procs.data[i])>0){
                strcpy(master,procs.data[i]);
            }
        }

        sprintf(master_path,"%s/%s",path,master);
    
        ret = zoo_get(zkhandle,master_path,0,ip_pid,&ip_pid_len,NULL);
        if(ret != ZOK){
            fprintf(stderr,"failed to get the data of path %s!\n",master_path);
        }else if(strcmp(ip_pid,localhost)==0){
            g_mode = MODE_MONITOR;
        }
        
    }

    for(i = 0; i < procs.count; ++i){
        free(procs.data[i]);
        procs.data[i] = NULL;
    }

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

int main(int argc, const char *argv[])  
{  
    int timeout = 30000;  
    char path_buffer[512];  
    int bufferlen=sizeof(path_buffer);  
  
    zoo_set_debug_level(ZOO_LOG_LEVEL_WARN); //设置日志级别,避免出现一些其他信息  

    get_option(argc,argv);

    zhandle_t* zkhandle = zookeeper_init(g_host,zktest_watcher_g, timeout, 0, (char *)"Monitor Test", 0);  

    if (zkhandle ==NULL)  
    {  
        fprintf(stderr, "Error when connecting to zookeeper servers...\n");  
        exit(EXIT_FAILURE);  
    }  
    
    char path[512]="/Monitor";
    
    int ret = zoo_exists(zkhandle,path,0,NULL); 
    if(ret != ZOK){
        ret = zoo_create(zkhandle,path,"1.0",strlen("1.0"),  
                          &ZOO_OPEN_ACL_UNSAFE,0,  
                          path_buffer,bufferlen);  
        if(ret != ZOK){
            fprintf(stderr,"failed to create the path %s!\n",path);
        }else{
            printf("create path %s successfully!\n",path);
        }
    }
  
    if(ret == ZOK && g_mode == MODE_WORKER){
        
        char localhost[512]={0};
        getlocalhost(localhost,sizeof(localhost));
        
        char child_path[512];
        sprintf(child_path,"%s/proc-",path);
        ret = zoo_create(zkhandle,child_path,localhost,strlen(localhost),  
                          &ZOO_OPEN_ACL_UNSAFE,ZOO_SEQUENCE|ZOO_EPHEMERAL,  
                          path_buffer,bufferlen);  
        if(ret != ZOK){
            fprintf(stderr,"failed to create the child_path %s,buffer:%s!\n",child_path,path_buffer);
        }else{
            printf("create child path %s successfully!\n",path_buffer);
        }
        choose_mater(zkhandle,path);

    }
    
    if(g_mode == MODE_MONITOR){
        show_list(zkhandle,path);
    }
    
    getchar();
    
    zookeeper_close(zkhandle); 

    return 0;
}  
