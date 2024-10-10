#ifndef SOCIAL_NETWORK_MICROSERVICES_SRC_HOMETIMELINESERVICE_HOMETIMELINEHANDLER_H_
#define SOCIAL_NETWORK_MICROSERVICES_SRC_HOMETIMELINESERVICE_HOMETIMELINEHANDLER_H_

#include <sw/redis++/redis++.h>

#include <future>
#include <iostream>
#include <string>
#include <pthread.h>

#include "../../gen-cpp/HomeTimelineService.h"
#include "../../gen-cpp/PostStorageService.h"
#include "../../gen-cpp/SocialGraphService.h"
#include "../ClientPool.h"
#include "../ThriftClient.h"
#include "../logger.h"
#include "../tracing.h"

using namespace sw::redis;
namespace social_network {
class HomeTimelineHandler : public HomeTimelineServiceIf {
 public:
  HomeTimelineHandler(Redis *,
                      ClientPool<ThriftClient<PostStorageServiceClient>> *,
                      ClientPool<ThriftClient<SocialGraphServiceClient>> *);


  HomeTimelineHandler(Redis *,Redis *,
      ClientPool<ThriftClient<PostStorageServiceClient>>*,
      ClientPool<ThriftClient<SocialGraphServiceClient>>*);


  HomeTimelineHandler(RedisCluster *,
                      ClientPool<ThriftClient<PostStorageServiceClient>> *,
                      ClientPool<ThriftClient<SocialGraphServiceClient>> *);
  ~HomeTimelineHandler() override = default;

  bool IsRedisReplicationEnabled();

  void ReadHomeTimeline(std::vector<Post> &, int64_t, int64_t, int, int,
                        const std::map<std::string, std::string> &) override;

  void WriteHomeTimeline(int64_t, int64_t, int64_t, int64_t,
                         const std::vector<int64_t> &,
                         const std::map<std::string, std::string> &) override;

 private:
     Redis *_redis_replica_pool;
     Redis *_redis_primary_pool;
     Redis *_redis_client_pool;
     RedisCluster *_redis_cluster_client_pool;
     ClientPool<ThriftClient<PostStorageServiceClient>> *_post_client_pool;
     ClientPool<ThriftClient<SocialGraphServiceClient>> *_social_graph_client_pool;
};

HomeTimelineHandler::HomeTimelineHandler(
    Redis *redis_pool,
    ClientPool<ThriftClient<PostStorageServiceClient>> *post_client_pool,
    ClientPool<ThriftClient<SocialGraphServiceClient>>
        *social_graph_client_pool) {
    _redis_primary_pool = nullptr;
    _redis_replica_pool = nullptr;
    _redis_client_pool = redis_pool;
    _redis_cluster_client_pool = nullptr;
    _post_client_pool = post_client_pool;
    _social_graph_client_pool = social_graph_client_pool;
}

HomeTimelineHandler::HomeTimelineHandler(
    RedisCluster *redis_pool,
    ClientPool<ThriftClient<PostStorageServiceClient>> *post_client_pool,
    ClientPool<ThriftClient<SocialGraphServiceClient>>
        *social_graph_client_pool) {
    _redis_primary_pool = nullptr;
    _redis_replica_pool = nullptr;
    _redis_client_pool = nullptr;
    _redis_cluster_client_pool = redis_pool; 
    _post_client_pool = post_client_pool;
    _social_graph_client_pool = social_graph_client_pool;
}

HomeTimelineHandler::HomeTimelineHandler(
    Redis *redis_replica_pool,
    Redis *redis_primary_pool,
    ClientPool<ThriftClient<PostStorageServiceClient>>* post_client_pool,
    ClientPool<ThriftClient<SocialGraphServiceClient>>
    * social_graph_client_pool) {
    _redis_primary_pool = redis_primary_pool;
    _redis_replica_pool = redis_replica_pool;
    _redis_client_pool = nullptr;
    _redis_cluster_client_pool = nullptr;
    _post_client_pool = post_client_pool;
    _social_graph_client_pool = social_graph_client_pool;
}

bool HomeTimelineHandler::IsRedisReplicationEnabled() {
    return (_redis_primary_pool || _redis_replica_pool);
}

void HomeTimelineHandler::WriteHomeTimeline(
    int64_t req_id, int64_t post_id, int64_t user_id, int64_t timestamp,
    const std::vector<int64_t> &user_mentions_id,
    const std::map<std::string, std::string> &carrier) {
  // Initialize a span
  TextMapReader reader(carrier);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "write_home_timeline_server", {opentracing::ChildOf(parent_span->get())});

  // Find followers of the user
  auto followers_span = opentracing::Tracer::Global()->StartSpan(
      "get_followers_client", {opentracing::ChildOf(&span->context())});
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  opentracing::Tracer::Global()->Inject(followers_span->context(), writer);

  auto social_graph_client_wrapper = _social_graph_client_pool->Pop();
  if (!social_graph_client_wrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
    se.message = "Failed to connect to social-graph-service";
    throw se;
  }
  auto social_graph_client = social_graph_client_wrapper->GetClient();
  std::vector<int64_t> followers_id;
  try {
    social_graph_client->GetFollowers(followers_id, req_id, user_id,
                                      writer_text_map);
  } catch (...) {
    LOG(error) << "Failed to get followers from social-network-service";
    _social_graph_client_pool->Remove(social_graph_client_wrapper);
    throw;
  }
  _social_graph_client_pool->Keepalive(social_graph_client_wrapper);
  followers_span->Finish();

  std::set<int64_t> followers_id_set(followers_id.begin(), followers_id.end());
  followers_id_set.insert(user_mentions_id.begin(), user_mentions_id.end());

  // Update Redis ZSet
  // Zset key: follower_id, Zset value: post_id_str, Zset score: timestamp_str
  auto redis_span = opentracing::Tracer::Global()->StartSpan(
      "write_home_timeline_redis_update_client",
      {opentracing::ChildOf(&span->context())});
  std::string post_id_str = std::to_string(post_id);

  {
    if (_redis_client_pool) {
      auto pipe = _redis_client_pool->pipeline(false);
      for (auto &follower_id : followers_id_set) {
        pipe.zadd(std::to_string(follower_id), post_id_str, timestamp,
                  UpdateType::NOT_EXIST);
      }
      try {
        auto replies = pipe.exec();
      } catch (const Error &err) {
        LOG(error) << err.what();
        throw err;
      }
    }
    
    else if (IsRedisReplicationEnabled()) {
        auto pipe = _redis_primary_pool->pipeline(false);
        for (auto& follower_id : followers_id_set) {
            pipe.zadd(std::to_string(follower_id), post_id_str, timestamp,
                UpdateType::NOT_EXIST);
        }
        try {
            auto replies = pipe.exec();
        }
        catch (const Error& err) {
            LOG(error) << err.what();
            throw err;
        }
    }
    
    else {
      // Create multi-pipeline that match with shards pool
      std::map<std::shared_ptr<ConnectionPool>, std::shared_ptr<Pipeline>> pipe_map;
      auto *shards_pool = _redis_cluster_client_pool->get_shards_pool();

      for (auto &follower_id : followers_id_set) {
        auto conn = shards_pool->fetch(std::to_string(follower_id));
        auto pipe = pipe_map.find(conn);
        if(pipe == pipe_map.end()) {//Not found, create new pipeline and insert
          auto new_pipe = std::make_shared<Pipeline>(_redis_cluster_client_pool->pipeline(std::to_string(follower_id), false));
          pipe_map.insert(make_pair(conn, new_pipe));
          auto *_pipe = new_pipe.get();
          _pipe->zadd(std::to_string(follower_id), post_id_str, timestamp,
                  UpdateType::NOT_EXIST);
        }else{//Found, use exist pipeline
          std::pair<std::shared_ptr<ConnectionPool>, std::shared_ptr<Pipeline>> found = *pipe;
          auto *_pipe = found.second.get();
          _pipe->zadd(std::to_string(follower_id), post_id_str, timestamp,
                  UpdateType::NOT_EXIST);
        }
      }
      // LOG(info) <<"followers_id_set items:" << followers_id_set.size()<<"; pipeline items:" << pipe_map.size();
      try {
        for(auto const &it : pipe_map) {
          auto _pipe = it.second.get();
          _pipe->exec();
        }

      } catch (const Error &err) {
        LOG(error) << err.what();
        throw err;
      }
    }
  }
  redis_span->Finish();
}


void HomeTimelineHandler::ReadHomeTimeline(
    std::vector<Post> &_return, int64_t req_id, int64_t user_id, int start_idx,
    int stop_idx, const std::map<std::string, std::string> &carrier) {
  // Initialize a span

  // sched_param sch_params;
  // sch_params.sched_priority = sched_get_priority_max(SCHED_FIFO);  // Use max priority for real-time
  
  // // Get the current thread handle
  // pthread_t this_thread = pthread_self();
  
  // // Set real-time scheduling with the FIFO policy
  // if (pthread_setschedparam(this_thread, SCHED_FIFO, &sch_params)) {
  //     std::cerr << "Failed to set thread scheduling: Insufficient permissions or incorrect settings." << std::endl;
  // } else {
  //     std::cout << "Thread set to real-time priority" << std::endl;
  // }

  int num_cores = sysconf(_SC_NPROCESSORS_ONLN);  // Get number of available CPU cores

  // Random number generation
  std::random_device rd;                           // Seed
  std::mt19937 gen(rd());                          // Random number engine
  std::uniform_int_distribution<> dist(0, num_cores - 1);

  int core_id = dist(gen);  // Generate a random number between 0 and num_cores - 1

  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);           // Clear the CPU set
  CPU_SET(core_id, &cpuset);   // Add the selected core to the CPU set
  
  pthread_t this_thread = pthread_self();  // Get the current thread

  if (pthread_setaffinity_np(this_thread, sizeof(cpu_set_t), &cpuset)) {
      std::cerr << "Failed to pin thread to core " << core_id << std::endl;
  } else {
      std::cout << "Thread pinned to core " << core_id << std::endl;
  }

  const char* env = getenv("DELAY");
  unsigned long delay_microseconds = env ? strtoul(env, NULL, 10) : 1000; // Default to 1000 if variable is not set

  // Capture the start time using std::chrono
  auto start = std::chrono::high_resolution_clock::now();

  // Busy-wait loop using std::chrono
  do {
      auto end = std::chrono::high_resolution_clock::now(); // Capture current time

      // Calculate the elapsed time in microseconds
      auto elapsed_time = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

      // asm("");  // Prevent compiler optimizations (same as in original code)

      if (elapsed_time >= delay_microseconds) {
          break;  // Exit the loop when the delay time is reached
      }

  } while (true);

  // unpin the thread
  CPU_ZERO(&cpuset);
  // Allow the thread to run on any CPU core
  for (int i = 0; i < num_cores; ++i) {
      CPU_SET(i, &cpuset);
  }
  
  if (pthread_setaffinity_np(this_thread, sizeof(cpu_set_t), &cpuset)) {
      std::cerr << "Failed to release CPU affinity" << std::endl;
  } else {
      std::cout << "Thread CPU affinity released (can run on any core)" << std::endl;
  }

  // // revert to normal scheduling
  // sch_params.sched_priority = 0;
  // if (pthread_setschedparam(this_thread, SCHED_OTHER, &sch_params)) {
  //     std::cerr << "Failed to set thread scheduling: Insufficient permissions or incorrect settings." << std::endl;
  // } else {
  //     std::cout << "Thread set to normal priority" << std::endl;
  // }

  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "read_home_timeline_server", {opentracing::ChildOf(parent_span->get())});
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  if (stop_idx <= start_idx || start_idx < 0) {
    return;
  }

  auto redis_span = opentracing::Tracer::Global()->StartSpan(
      "read_home_timeline_redis_find_client",
      {opentracing::ChildOf(&span->context())});

  std::vector<std::string> post_ids_str;
  try {
    if (_redis_client_pool) {
      _redis_client_pool->zrevrange(std::to_string(user_id), start_idx,
                                    stop_idx - 1,
                                    std::back_inserter(post_ids_str));
    }
    else if (IsRedisReplicationEnabled()) {
        _redis_replica_pool->zrevrange(std::to_string(user_id), start_idx,
                                       stop_idx - 1,
                                       std::back_inserter(post_ids_str));
    }
    
    else {
      _redis_cluster_client_pool->zrevrange(std::to_string(user_id), start_idx,
                                            stop_idx - 1,
                                            std::back_inserter(post_ids_str));
    }
  } catch (const Error &err) {
    LOG(error) << err.what();
    throw err;
  }
  redis_span->Finish();

  std::vector<int64_t> post_ids;
  for (auto &post_id_str : post_ids_str) {
    post_ids.emplace_back(std::stoul(post_id_str));
  }

  auto post_client_wrapper = _post_client_pool->Pop();
  if (!post_client_wrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
    se.message = "Failed to connect to post-storage-service";
    throw se;
  }
  auto post_client = post_client_wrapper->GetClient();
  try {
    post_client->ReadPosts(_return, req_id, post_ids, writer_text_map);
  } catch (...) {
    _post_client_pool->Remove(post_client_wrapper);
    LOG(error) << "Failed to read posts from post-storage-service";
    throw;
  }
  _post_client_pool->Keepalive(post_client_wrapper);
  span->Finish();
}

}  // namespace social_network

#endif  // SOCIAL_NETWORK_MICROSERVICES_SRC_HOMETIMELINESERVICE_HOMETIMELINEHANDLER_H_
