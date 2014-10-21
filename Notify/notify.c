#include<stdio.h>  
#include<string.h>  
#include<unistd.h>
#include"zookeeper.h"  
#include"zookeeper_log.h"  

char g_host[512]= "172.17.0.36:2181";  
char g_path[512]= "/Notify";
int g_monitor_child = 0;

//watch function when child list changed
void zktest_watcher_g(zhandle_t* zh, int type, int state, const char* path, void* watcherCtx);
void show_notify(zhandle_t *zkhandle,const char *path);
//show all process ip:pid
void show_list(zhandle_t *zkhandle,const char *path);

void print_usage();
void get_option(int argc,const char* argv[]);

/**********unitl*********************/  
void print_usage()
{
    printf("Usage : [notify] [-h] [-c] [-p path][-s ip:port] \n");
    printf("        -h Show help\n");
    printf("        -p path\n");
    printf("        -c monitor the child nodes\n");
    printf("        -s zookeeper server ip:port\n");
    printf("For example:\n");
    printf("notify -s172.17.0.36:2181 -p /Notify\n");
}
 
void get_option(int argc,const char* argv[])
{
	extern char    *optarg;
	int            optch;
	int            dem = 1;
	const char    optstring[] = "hcp:s:";
    
    
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
        case 'c':
            g_monitor_child = 1;
            break;
        case 's':
            strncpy(g_host,optarg,sizeof(g_host));
            break;
        case 'p':
            strncpy(g_path,optarg,sizeof(g_path));
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

    if(type == ZOO_CHANGED_EVENT &&
       state == ZOO_CONNECTED_STATE &&
       g_monitor_child == 0){
       
        show_notify(zh,g_path);
    }else if(type == ZOO_CHILD_EVENT &&
            state == ZOO_CONNECTED_STATE &&
            g_monitor_child == 1){

        show_list(zh,g_path);
    }else if(type == ZOO_CHANGED_EVENT &&
            state == ZOO_CONNECTED_STATE &&
            g_monitor_child == 1){
    
        show_list(zh,g_path);
    }
} 
void show_notify(zhandle_t *zkhandle,const char *path)
{
    char notify_buffer[1024]={0};
    int  notify_len = sizeof(notify_buffer);
    
    int ret = zoo_get(zkhandle,g_path,1,notify_buffer,&notify_len,NULL);
    if(ret != ZOK){
        fprintf(stderr,"failed to get the data of path %s!\n",g_path);
    }else{
        printf("Notice:%s\n",notify_buffer);
    }
}
void show_list(zhandle_t *zkhandle,const char *path)
{

    struct String_vector children;
    int i = 0;
    int ret = zoo_get_children(zkhandle,path,1,&children);
        
    if(ret == ZOK){
        char child_path[512] ={0};
        char notify_buffer[1024] = {0};
        int notify_len = sizeof(notify_buffer);

        printf("--------------\n");
        for(i = 0; i < children.count; ++i){
            sprintf(child_path,"%s/%s",g_path,children.data[i]);
            ret = zoo_get(zkhandle,child_path,1,notify_buffer,&notify_len,NULL);
            if(ret != ZOK){
                fprintf(stderr,"failed to get the data of path %s!\n",child_path);
            }else{
                printf("%s:%s\n",children.data[i],notify_buffer);
            }
        }
    }else{
        fprintf(stderr,"failed to get the children of path %s!\n",path);
    }

    for(i = 0; i < children.count; ++i){
        free(children.data[i]);
        children.data[i] = NULL;
    }
}

int main(int argc, const char *argv[])  
{  
    int timeout = 30000;  
    char path_buffer[512];  
    int bufferlen=sizeof(path_buffer);  
  
    zoo_set_debug_level(ZOO_LOG_LEVEL_WARN); //设置日志级别,避免出现一些其他信息  

    get_option(argc,argv);

    zhandle_t* zkhandle = zookeeper_init(g_host,zktest_watcher_g, timeout, 0, (char *)"Notify Test", 0);  

    if (zkhandle ==NULL)  
    {  
        fprintf(stderr, "Error when connecting to zookeeper servers...\n");  
        exit(EXIT_FAILURE);  
    }  
    
    int ret = zoo_exists(zkhandle,g_path,0,NULL); 
    if(ret != ZOK){
        ret = zoo_create(zkhandle,g_path,"1.0",strlen("1.0"),  
                          &ZOO_OPEN_ACL_UNSAFE,0,  
                          path_buffer,bufferlen);  
        if(ret != ZOK){
            fprintf(stderr,"failed to create the path %s!\n",g_path);
        }else{
            printf("create path %s successfully!\n",g_path);
        }
    }
  
    if(ret == ZOK && g_monitor_child == 0){
        show_notify(zkhandle,g_path);
    }else if(ret == ZOK && g_monitor_child == 1){
        show_list(zkhandle,g_path);
    }
    
    getchar();
    
    zookeeper_close(zkhandle); 

    return 0;
}  
