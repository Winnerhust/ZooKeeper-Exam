#include<stdio.h>  
#include<string.h>  
#include<unistd.h>
#include"zookeeper.h"  
#include"zookeeper_log.h"  

char g_host[512]= "172.17.0.36:2181";  
char g_path[512]= "/Queue";
char g_value[512]="msg";
enum MODE{PUSH_MODE,POP_MODE} g_mode;

void print_usage();
void get_option(int argc,const char* argv[]);

/**********unitl*********************/  
void print_usage()
{
    printf("Usage : [myqueue] [-h] [-m mode] [-p path ] [-v value][-s ip:port] \n");
    printf("        -h Show help\n");
    printf("        -p Queue path\n");
    printf("        -m mode:push or pop\n");
    printf("        -v the value you want to push\n");
    printf("        -s zookeeper server ip:port\n");
    printf("For example:\n");
    printf("    push the message \"Hello\" into the queue Queue:\n");
    printf("        >myqueue -s172.17.0.36:2181 -p /Queue -m push -v Hello\n");
    printf("    pop one message from the queue Queue:\n");
    printf("        >myqueue -s172.17.0.36:2181 -p /Queue -m pop\n");
}
 
void get_option(int argc,const char* argv[])
{
	extern char    *optarg;
	int            optch;
	int            dem = 1;
	const char    optstring[] = "hv:m:p:s:";
    
        
    g_mode = PUSH_MODE;
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
            if(strcasecmp(optarg,"push")==0){
                g_mode = PUSH_MODE;
            }else{
                g_mode = POP_MODE;
            }
            break;
        case 's':
            strncpy(g_host,optarg,sizeof(g_host));
            break;
        case 'p':
            strncpy(g_path,optarg,sizeof(g_path));
            break;
        case 'v':
            strncpy(g_value,optarg,sizeof(g_value));
            break;
		default:
			break;
		}
	}
} 

int push(zhandle_t *zkhandle,const char *path,char *element)
{
    char child_path[512] = {0};
    char path_buffer[512] = {0};
    int bufferlen = sizeof(path_buffer);

    sprintf(child_path,"%s/queue-",path);
    int ret = zoo_create(zkhandle,child_path,element,strlen(element),  
                     &ZOO_OPEN_ACL_UNSAFE,ZOO_SEQUENCE,  
                     path_buffer,bufferlen);  
    if(ret != ZOK){
        fprintf(stderr,"failed to create the path %s!\n",path);
    }else{
        printf("create path %s successfully!\n",path);
    }

    return ret;
}

int pop(zhandle_t *zkhandle,const char *path,char *element,int *len)
{
    int i = 0;
    struct String_vector children;
    int ret = zoo_get_children(zkhandle,path,0,&children);
    

    if(ret != ZOK){
        fprintf(stderr,"failed to create the path %s!\n",path);
    }else if (children.count == 0){
        strcpy(element,"");
        *len = 0;
        ret = -1;
    }else{
        char *min = children.data[0];
        for(i = 0; i < children.count; ++i){
            printf("%s:%s\n",min,children.data[i]);
            if(strcmp(min,children.data[i]) > 0){
                min = children.data[i];
            }
        }
        if(min != NULL){
            char child_path[512]={0};
            sprintf(child_path,"%s/%s",path,min);
            ret = zoo_get(zkhandle,child_path,0,element,len,NULL);

            if(ret != ZOK){
                fprintf(stderr,"failed to get data of the path %s!\n",child_path);
            }else{
                ret = zoo_delete(zkhandle,child_path, -1);
                
                if(ret != ZOK){
                    fprintf(stderr,"failed to delete the path %s!\n",child_path);
                }
            }
        }
    }
    
    for(i = 0; i < children.count; ++i){
        free(children.data[i]);
        children.data[i] = NULL;
    }
    

    return ret;
}

int front(zhandle_t *zkhandle,char *path,char *element,int *len)
{
    int i = 0;
    struct String_vector children;
    int ret = zoo_get_children(zkhandle,path,0,&children);

    if(ret != ZOK){
        fprintf(stderr,"failed to create the path %s!\n",path);
    }else if(children.count == 0){
        strcpy(element,"");
        *len = 0;
        ret = -1;
    }else{
        char *min = NULL;
        for(i = 0; i < children.count; ++i){
            if(strcmp(min,children.data[i]) > 0){
                min = children.data[i];
            }
        }
        if(min != NULL){
            char child_path[512]={0};
            sprintf(child_path,"%s/%s",path,min);
            ret = zoo_get(zkhandle,child_path,0,element,len,NULL);

            if(ret != ZOK){
                fprintf(stderr,"failed to get data of the path %s!\n",child_path);
            }
        }
    }
    
    for(i = 0; i < children.count; ++i){
        free(children.data[i]);
        children.data[i] = NULL;
    }

    return ret;

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
    
    if(g_mode == PUSH_MODE){
        push(zkhandle,g_path,g_value); 
        printf("push:%s\n",g_value);
    }else{
        int len = sizeof(g_value);
        ret = pop(zkhandle,g_path,g_value,&len) ;
       
        if(ret == ZOK){
            printf("pop:%s\n",g_value);
        }else if( ret == -1){
            printf("queue is empty\n");
        }
    }
    
  

    zookeeper_close(zkhandle); 

    return 0;
} 
