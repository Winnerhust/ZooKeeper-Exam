#include<stdio.h>  
#include<string.h>  
#include<unistd.h>
#include"zookeeper.h"  
#include"zookeeper_log.h"  

char g_host[512]= "172.17.0.36:2181";  
char g_path[512]= "/Lock";

typedef struct Lock
{
    char lockpath[1024];
    char selfpath[1024];
}Lock;

void print_usage();
void get_option(int argc,const char* argv[]);

/**********unitl*********************/  
void print_usage()
{
    printf("Usage : [mylock] [-h]  [-p path][-s ip:port] \n");
    printf("        -h Show help\n");
    printf("        -p lock path\n");
    printf("        -s zookeeper server ip:port\n");
    printf("For example:\n");
    printf("    mylock -s172.17.0.36:2181 -p /Lock\n");
}
 
void get_option(int argc,const char* argv[])
{
	extern char    *optarg;
	int            optch;
	int            dem = 1;
	const char    optstring[] = "hp:s:";
    
    
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

Lock *create_lock(zhandle_t *zkhandle,const char *path)
{
    char path_buffer[512]={0};
    int bufferlen = sizeof(path_buffer);
    Lock * lock = NULL;

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
    if(ret == ZOK){
        char child_path[512];
        sprintf(child_path,"%s/lock-",path);
        ret = zoo_create(zkhandle,child_path,"1.0",strlen("1.0"),  
                          &ZOO_OPEN_ACL_UNSAFE,ZOO_SEQUENCE|ZOO_EPHEMERAL,  
                          path_buffer,bufferlen);  
        if(ret != ZOK){
            fprintf(stderr,"failed to create the path %s!\n",path);
        }else{
            printf("create path %s successfully!\n",path);
        }
    }
    if(ret == ZOK){
        lock = (Lock *)malloc(sizeof(Lock));
        
        strcpy(lock->lockpath,path);
        strcpy(lock->selfpath,path_buffer);
    }

    return lock;
}

int try_lock(zhandle_t *zkhandle,Lock *lock)
{
    struct String_vector children;
    int i = 0;
    int ret = zoo_get_children(zkhandle,lock->lockpath,0,&children);

    if(ret != ZOK){
        fprintf(stderr,"error when get children of path %s\n",lock->lockpath);
        ret = -1;
    }else{
        char *myseq = rindex(lock->selfpath,'/');
        if (myseq != NULL) myseq += 1;
        
        ret = 1;
        for(i = 0; i < children.count; ++i){
            if(strcmp(children.data[i],myseq) < 0){
                ret = 0;
                break;
            }            
        }

        for(i = 0; i < children.count; ++i){
            free(children.data[i]);
            children.data[i] = NULL;
        }
    }

    return ret;
}

Lock *lock(zhandle_t *zkhandle,const char *path)
{
    int ret ;
    Lock *lock = create_lock(zkhandle,path);
    if(lock != NULL){
        while((ret = try_lock(zkhandle,lock)) == 0){
            sleep(1);
        }
    }else{
        fprintf(stderr,"error when create lock %s.\n",path);
    }

    return lock;
}

int unlock(zhandle_t *zkhandle,Lock * *lock)
{
    if(*lock){
        int ret = zoo_delete(zkhandle,(*lock)->selfpath,-1);
        if(ret != ZOK){
            fprintf(stderr,"error when release lock %s.\n",(*lock)->selfpath);
        }
        free(*lock);
        *lock = NULL;
        
        return ret;
    }

    return ZOK;
}

int main(int argc, const char *argv[])  
{  
    int timeout = 30000;  
    char path_buffer[512];  
    int bufferlen=sizeof(path_buffer);  
  
    zoo_set_debug_level(ZOO_LOG_LEVEL_WARN); //设置日志级别,避免出现一些其他信息  

    get_option(argc,argv);

    zhandle_t* zkhandle = zookeeper_init(g_host,NULL, timeout, 0, (char *)"lock Test", 0);  

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
  
    if(ret == ZOK ){
       Lock *mylock = lock(zkhandle,g_path);
       
        if(mylock){
            printf("get lock of %s.\n",g_path);
            printf("self path is %s.\n",mylock->selfpath);
            
            printf("do something....\n");
            getchar();

            unlock(zkhandle,&mylock);
        }    
    }
    
    zookeeper_close(zkhandle); 

    return 0;
} 
