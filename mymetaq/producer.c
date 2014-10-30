#include<stdio.h>  
#include<stdlib.h>  
#include<string.h>  
#include<unistd.h>
#include"zookeeper.h"  
#include"zookeeper_log.h"  

char g_host[512]= "172.17.0.36:2181";  
char g_topic[512]= "MyTopic";
char g_msg[512]="Hello,World";
int  g_repeated_num = 1;
int  g_partition_num = 4;

void print_usage();
void get_option(int argc,const char* argv[]);

/**********unitl*********************/  
void print_usage()
{
    printf("Usage : [produce] [-h] [-t topic] [-m msg] [-r reapted_num] [-n partition_num] [-s ip:port] \n");
    printf("        -h Show help\n");
    printf("        -t topic name\n");
    printf("        -m the message content\n");
    printf("        -r send repeated times\n");
    printf("        -n default partition number\n");
    printf("        -s zookeeper server ip:port\n");
    printf("For example:\n");
    printf("    send the message \"Hello,World\" to the breaker:\n");
    printf("        >produce -t MyTopic -m Hello,World -n 3 -s 172.17.0.36:2181\n");
}
 
void get_option(int argc,const char* argv[])
{
	extern char    *optarg;
	int            optch;
	int            dem = 1;
	const char    optstring[] = "ht:r:m:n:s:";
    
        
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
            strncpy(g_msg,optarg,sizeof(g_msg));
            break;
        case 's':
            strncpy(g_host,optarg,sizeof(g_host));
            break;
        case 't':
            strncpy(g_topic,optarg,sizeof(g_topic));
            break;
        case 'r':
            if(optarg != NULL){
                g_repeated_num = atoi(optarg);
            }
            break;
        case 'n':
            if(optarg != NULL){
                g_partition_num= atoi(optarg);
            }
            break;
		default:
			break;
		}
	}
} 

//同步方式创建节点  
void create(zhandle_t *zkhandle,char *path,int flag,const char *ctx)  
{  
    char path_buffer[512];  
    int bufferlen=sizeof(path_buffer);  
    

    int ret = zoo_exists(zkhandle,path,0,NULL); 
    if(ret != ZOK){
        ret = zoo_create(zkhandle,path,ctx,strlen(ctx),
                          &ZOO_OPEN_ACL_UNSAFE,flag,
                          path_buffer,bufferlen);
  
        if (ret != ZOK){  
            printf("节点创建失败:%s \n",path);  
            exit(EXIT_FAILURE);  
        }else {  
            printf("创建的节点名称为：%s\n",path_buffer);  
            printf("创建的节点值为：%s\n",(char *)ctx);  
        }
    }else{
        printf("path:%s 已经存在,无需创建\n",path);
    }  
}  


int main(int argc, const char *argv[])  
{  
    int timeout = 30000;  
    char path_buffer[512];  
    int bufferlen=sizeof(path_buffer);  
  
    zoo_set_debug_level(ZOO_LOG_LEVEL_WARN); //设置日志级别,避免出现一些其他信息  

    get_option(argc,argv);

    zhandle_t* zkhandle = zookeeper_init(g_host,NULL, timeout, 0, (char *)"Load Balancing Test", 0);  

    if (zkhandle ==NULL){ 
        fprintf(stderr, "Error when connecting to zookeeper servers...\n");  
        exit(EXIT_FAILURE);  
    }  
    
    char breaker_path[512] = "/Breaker";
    char topic_path[512] = {0};
    char partition_path[512] = {0};
    
    sprintf(topic_path,"%s/%s",breaker_path,g_topic);

    //init environment
    create(zkhandle,breaker_path,0,"");  
    create(zkhandle,topic_path,0,"");  

    int i = 0;
    for(i = 0; i < g_partition_num ; ++i){
        sprintf(partition_path,"%s/Partition-%d",topic_path,i);
        create(zkhandle,partition_path,0,"");
    }
    
    //send message by rand time
    srand(time(NULL));
    char msg_path[512] = {0};
    for(i = 0; i <  g_repeated_num; ++i){
        sprintf(msg_path,"%s/Partition-%d/msg-",topic_path,i % g_partition_num);
        // create seq msg node
        create(zkhandle,msg_path,ZOO_SEQUENCE,g_msg);
        int time = rand()%10;
        printf("sleep time:%d\n",time);
        sleep(time);
    }



    zookeeper_close(zkhandle); 

    return 0;
}
