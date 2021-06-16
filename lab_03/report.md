## Report on the ClickHouse lab

### What have you done?

I have created next **tables**:
 * *followers*;
 * *friends*;
 * *likes*;
 * *posts*;
 * *user_profiles*;
 
**distributed tables**:
 * *distr_followers*;
 * *distr_friends*;
 * *distr_likes*;
 * *distr_posts*;
 * *distr_user_profiles*;
 
**kafka tables**:
 * *kafka_followers*;
 * *kafka_friends*;
 * *kafka_likes*;
 * *kafka_posts*;
 * *kafka_user_profiles*;
 
and **materialized views**:
 * *mv_kafka_followers*;
 * *mv_kafka_friends*;
 * *mv_kafka_likes*;
 * *mv_kafka_posts*;
 * *mv_kafka_user_profiles*.

### Listing

**Kubernetes useful commands**

```shell script
kubectl create -f *.yaml
kubectl delete -f *.yaml

kubectl get pods
kubectl get svc

kubectl exec --stdin --tty <pod_name> -- /bin/bash
```

---

**Kafka useful commands**

```shell script
# Topics list
kafka-topics.sh --zookeeper zoo1:2181 -list

# Create topic
kafka-topics.sh --create --zookeeper zoo1:2181 --topic topic-followers --partitions 1 --replication-factor 1
kafka-topics.sh --create --zookeeper zoo1:2181 --topic topic-friends --partitions 1 --replication-factor 1
kafka-topics.sh --create --zookeeper zoo1:2181 --topic topic-likes --partitions 1 --replication-factor 1
kafka-topics.sh --create --zookeeper zoo1:2181 --topic topic-posts --partitions 1 --replication-factor 1
kafka-topics.sh --create --zookeeper zoo1:2181 --topic topic-user-profiles --partitions 1 --replication-factor 1

# Purge topic
kafka-topics.sh --zookeeper zoo1:2181 --alter --topic <topic_name> --config retention.ms=1000
kafka-configs.sh --zookeeper zoo1:2181 --entity-type topics --alter --entity-name <topic_name> --add-config retention.ms=1000
# Wait couple of minutes and call
kafka-configs.sh --zookeeper zoo1:2181 --entity-type topics --alter --entity-name <topic_name> --delete-config retention.ms=1000
```

---

**Database**

```clickhouse
CREATE DATABASE IF NOT EXISTS sultan_db ON CLUSTER kube_clickhouse_cluster;
```

---

**Tables**

1. *followers*
    ```clickhouse
    CREATE TABLE sultan_db.followers ON CLUSTER kube_clickhouse_cluster
    (
        profile_id     Int64,
        follower_id    Int64,
        ctime          DateTime
    )
    ENGINE = MergeTree()
    PARTITION BY toYear(ctime)
    ORDER BY (profile_id, follower_id);
    ```

2. *friends* 
    ```clickhouse
    CREATE TABLE sultan_db.friends ON CLUSTER kube_clickhouse_cluster
    (
        user_id        Int64,
        friend_id      Int64,
        ctime          DateTime
    )
    ENGINE = MergeTree()
    PARTITION BY toYear(ctime)
    ORDER BY (user_id, friend_id);
    ```

3. *likes*
    ```clickhouse
    CREATE TABLE sultan_db.likes ON CLUSTER kube_clickhouse_cluster
    (
        item_type      String,
        owner_id       Int64,
        item_id        Int64,
        liker_id       Int64,
        ctime          DateTime,
        like_date      DateTime,
        post_date      DateTime  
    )
    ENGINE = MergeTree()
    PARTITION BY toYear(post_date)
    ORDER BY (liker_id, owner_id, item_id);
    ```

4. *posts*
    ```clickhouse
    CREATE TABLE sultan_db.posts ON CLUSTER kube_clickhouse_cluster
    (
        ctime           DateTime,
        date            DateTime,
        post_id         Int64,
        from_id         Int64,
        owner_id        Int64,
        comments_count  Nullable(Int64),
        likes_count     Nullable(Int64),
        reposts_count   Nullable(Int64),
        views_count     Nullable(Int64),
        text            Nullable(String),
        signed_by       Nullable(Int64),
        post_type       Nullable(String),
        reposted_from_owner_id Nullable(Int64),
        reposted_from_post_id  Nullable(Int64),
        geo             Nullable(Int64),
        geo_lat         Nullable(Float64),
        geo_lon         Nullable(Float64),
        geo_hash        Nullable(Float64),
    
        photo_attachments Nested
        (
            owner_id    Nullable(Int64),
            photo_id    Nullable(Int64),
            size        Nullable(Float64),
            url         Nullable(String),
            geo_lat     Nullable(Float64),
            geo_lon     Nullable(Float64),
            geo_hash    Nullable(Float64)
        ),
    
        video_attachments Nested
        (
            owner_id    Nullable(Int64),
            video_id    Nullable(Int64),
            views       Nullable(Int64)
        ),
    
        audio_attachments Nested
        (
            owner_id    Nullable(Int64),
            audio_id    Nullable(Int64),
            artist      Nullable(String),
            title       Nullable(String)
        ),
    
        doc_attachments Nested
        (
            owner_id    Nullable(Int64),
            doc_id      Nullable(Int64),
            title       Nullable(String),
            size        Nullable(Float64),
            url         Nullable(String),
            type        Nullable(UInt8)
        ),
    
        link_attachments Nested
        (
            title       Nullable(String),
            url         Nullable(String)
        ),
    
         page_attachments Nested
        (
            group_id    Nullable(Int64),
            page_id     Nullable(Int64)
        ),
    
        sticker_attachments Nested
        (
            sticker_id  Nullable(Int64)
        ),
    
        photos_attachments_count    Nullable(Int64),
        videos_attachments_count    Nullable(Int64),
        audios_attachments_count    Nullable(Int64),
        docs_attachments_count      Nullable(Int64),
        links_attachments_count     Nullable(Int64),
        pages_attachments_count     Nullable(Int64),
        stickers_attachments_count  Nullable(Int64)
    )
    ENGINE = MergeTree()
    PARTITION BY toYear(ctime)
    ORDER BY (owner_id, post_id);
    ```

5. *user_profiles*
    ```clickhouse
    CREATE TABLE sultan_db.user_profiles ON CLUSTER kube_clickhouse_cluster
    (
        ctime                   DateTime,
        id                      Int64,
        first_name              String,
        last_name               String,
        screen_name             Nullable(String),
        maiden_name             Nullable(String),
        nickname                Nullable(String),
        bdate                   Nullable(String),
        birth_date              Nullable(Float64),
        sex                     Int16,
        deactivated             Int8,
        is_closed               Nullable(Float64),
        verified                Int8,
        followers_count         Nullable(Float64),
        status                  Nullable(String),
        city_id                 Int32,
        city_title              Nullable(String),
        country_id              Int32,
        country_title           Nullable(String),
        mobile_phone            Nullable(String),
        home_phone              Nullable(String),
        tv                      Nullable(String),
        twitter                 Nullable(String),
        livejournal             Nullable(String),
        facebook                Nullable(String),
        site                    Nullable(String),
        skype                   Nullable(String),
        instagram               Nullable(String),
        about                   Nullable(String),
        activities              Nullable(String),
        books                   Nullable(String),
        home_town               Nullable(String),
        interests               Nullable(String),
        movies                  Nullable(String),
        music                   Nullable(String),
        games                   Nullable(String),
        quotes                  Nullable(String),
        domain                  Nullable(String),
        personal_alcohol        Nullable(Float64),
        personal_inspired_by    Nullable(String),
        personal_langs Array(   Nullable(String)),
        personal_life_main      Nullable(Float64),
        personal_people_main    Nullable(Float64),
        personal_political      Nullable(Float64),
        personal_religion       Nullable(String),
        personal_smoking        Nullable(Float64),
        relation                Nullable(Float64),
        relation_partner_first_name Nullable(String),
        relation_partner_id         Nullable(Float64),
        relation_partner_last_name  Nullable(String),
        photo_id                Nullable(String),
        photo_max_url           Nullable(String),
        crop_photo_album_id     Nullable(Float64),
        crop_photo_date         Nullable(DateTime),
        crop_photo_id           Nullable(Float64),
        crop_photo_lat          Nullable(Float64),
        crop_photo_long         Nullable(Float64),
        crop_photo_owner_id     Nullable(Float64),
        crop_photo_max          Nullable(Float64),
        crop_photo_max_url      Nullable(String),
        crop_photo_post_id      Nullable(Float64),
        crop_photo_text         Nullable(String),                   
        occupation_id           Nullable(Float64),
        occupation_name         Nullable(String),
        occupation_type         Nullable(String),
        education_form          Nullable(Float64),
        education_status        Nullable(String),
        faculty                 Nullable(Float64),
        faculty_name            Nullable(String),
        graduation              Nullable(Float64),
        university              Nullable(Float64),
        university_name         Nullable(String),
    
        relatives Nested
        (
            user_id             Nullable(Int64),
            name                Nullable(String),
            type                Nullable(String)
        ),
    
        career Nested
        (
            company             Nullable(String),
            group_id            Nullable(Int64),
            city_id             Nullable(Int32),
            country_id          Nullable(Int32),
            position            Nullable(String),
            from                Nullable(UInt32),
            until               Nullable(UInt32)
        ),
    
        schools Nested
        (
            city_id             Nullable(Int32),
            class               Nullable(String),
            country_id          Nullable(Int32),
            id                  Nullable(Int32),
            name                Nullable(String),
            speciality          Nullable(String),
            type                Nullable(UInt8),
            type_str            Nullable(String),
            year_from           Nullable(UInt16),
            year_graduated      Nullable(UInt16),
            year_to             Nullable(UInt16)
        ),
    
        universities Nested
        (
            chair               Nullable(UInt32),
            chair_name          Nullable(String),
            education_form      Nullable(String),
            education_status    Nullable(String),
            faculty             Nullable(UInt32),
            faculty_name        Nullable(String),
            graduation          Nullable(UInt32),
            id                  Nullable(Int32),
            name                Nullable(String)
        )
    )
    ENGINE = MergeTree()
    ORDER BY (id);
    ```

---

**Distributed tables**

1. *followers*
    ```clickhouse
    CREATE TABLE sultan_db.distr_followers ON CLUSTER kube_clickhouse_cluster 
    AS sultan_db.followers
    ENGINE = Distributed(kube_clickhouse_cluster, sultan_db, followers, xxHash64(profile_id));
    ```
   
2. *friends*
    ```clickhouse
    CREATE TABLE sultan_db.distr_friends ON CLUSTER kube_clickhouse_cluster 
    AS sultan_db.friends
    ENGINE = Distributed(kube_clickhouse_cluster, sultan_db, friends, xxHash64(user_id));
    ```
    
3. *likes*
    ```clickhouse
    CREATE TABLE sultan_db.distr_likes ON CLUSTER kube_clickhouse_cluster 
    AS sultan_db.likes
    ENGINE = Distributed(kube_clickhouse_cluster, sultan_db, likes, xxHash64(liker_id));
    ```
   
4. *posts*
    ```clickhouse
    CREATE TABLE sultan_db.distr_posts ON CLUSTER kube_clickhouse_cluster 
    AS sultan_db.posts
    ENGINE = Distributed(kube_clickhouse_cluster, sultan_db, posts, xxHash64(from_id));
    ```
    
5. *user_profiles*
    ```clickhouse
    CREATE TABLE sultan_db.distr_user_profiles ON CLUSTER kube_clickhouse_cluster 
    AS sultan_db.user_profiles
    ENGINE = Distributed(kube_clickhouse_cluster, sultan_db, user_profiles, xxHash64(id));
    ```

---

**Kafka tables**

1. *followers*
    ```clickhouse
    CREATE TABLE sultan_db.kafka_followers ON CLUSTER kube_clickhouse_cluster
    AS sultan_db.followers 
    ENGINE = Kafka('kafka-svc:9092', 'topic-followers', 'group1', 'JSONEachRow');
    ```
   
2. *friends*
    ```clickhouse
    CREATE TABLE sultan_db.kafka_friends ON CLUSTER kube_clickhouse_cluster
    AS sultan_db.friends
    ENGINE = Kafka('kafka-svc:9092', 'topic-friends', 'group2', 'JSONEachRow');
    ```
    
3. *likes*
    ```clickhouse
    CREATE TABLE sultan_db.kafka_likes ON CLUSTER kube_clickhouse_cluster
    AS sultan_db.likes
    ENGINE = Kafka('kafka-svc:9092', 'topic-likes', 'group3', 'JSONEachRow');
    ```
   
4. *posts*
    ```clickhouse
    CREATE TABLE sultan_db.kafka_posts ON CLUSTER kube_clickhouse_cluster 
    AS sultan_db.posts
    ENGINE = Kafka('kafka-svc:9092', 'topic-posts', 'group4', 'JSONEachRow');
    ```
    
5. *user_profiles*
    ```clickhouse
    CREATE TABLE sultan_db.kafka_user_profiles ON CLUSTER kube_clickhouse_cluster 
    AS sultan_db.user_profiles
    ENGINE = Kafka('kafka-svc:9092', 'topic-user-profiles', 'group5', 'JSONEachRow');
    ```

---

**Materialized views**

1. *followers*
    ```clickhouse
    CREATE MATERIALIZED VIEW sultan_db.mv_kafka_followers ON CLUSTER kube_clickhouse_cluster 
    TO sultan_db.distr_followers
    AS SELECT * 
    FROM sultan_db.kafka_followers;
    ```
   
2. *friends*
    ```clickhouse
    CREATE MATERIALIZED VIEW sultan_db.mv_kafka_friends ON CLUSTER kube_clickhouse_cluster 
    TO sultan_db.distr_friends
    AS SELECT * 
    FROM sultan_db.kafka_friends;
    ```
    
3. *likes*
    ```clickhouse
    CREATE MATERIALIZED VIEW sultan_db.mv_kafka_likes ON CLUSTER kube_clickhouse_cluster 
    TO sultan_db.distr_likes
    AS SELECT * 
    FROM sultan_db.kafka_likes;
    ```
   
4. *posts*
    ```clickhouse
    CREATE MATERIALIZED VIEW sultan_db.mv_kafka_posts ON CLUSTER kube_clickhouse_cluster 
    TO sultan_db.distr_posts
    AS SELECT * 
    FROM sultan_db.kafka_posts;
    ```
    
5. *user_profiles*
    ```clickhouse
    CREATE MATERIALIZED VIEW sultan_db.mv_kafka_user_profiles ON CLUSTER kube_clickhouse_cluster 
    TO sultan_db.distr_user_profiles
    AS SELECT * 
    FROM sultan_db.kafka_user_profiles;
    ```
   
---

**Kafka producers**

1. *followers*
    ```jupyterpython
    #%%
    ! head -{row_count} ~/shared-data/clickhouse_data/followers.json > ~/sample_followers.json
    #%%
    followers_producer = KafkaProducer(
        bootstrap_servers="kafka-svc:9092", 
        value_serializer=str.encode
    )
    followers_topic = "topic-followers"
    #%%
    with open("sample_followers.json") as f_src:
        for i, line in enumerate(f_src.readlines()):
            followers_producer.send(followers_topic, line)
            print(f"\rSent {i + 1}", end=str())
    ```

2. *friends*
    ```jupyterpython
    #%%
    ! head -{row_count} ~/shared-data/clickhouse_data/friends.json > ~/sample_friends.json
    #%%
    friends_producer = KafkaProducer(
        bootstrap_servers="kafka-svc:9092", 
        value_serializer=str.encode
    )
    friends_topic = "topic-friends"
    #%%
    with open("sample_friends.json") as f_src:
        for i, line in enumerate(f_src.readlines()):
            friends_producer.send(friends_topic, line)
            print(f"\rSent {i + 1}", end=str())
    ```

3. *likes*
    ```jupyterpython
    #%%
    ! head -{row_count} ~/shared-data/clickhouse_data/likes.json > ~/sample_likes.json
    #%%
    likes_producer = KafkaProducer(
        bootstrap_servers="kafka-svc:9092", 
        value_serializer=str.encode
    )
    likes_topic = "topic-likes"
    #%%
    with open("sample_likes.json") as f_src:
        for i, line in enumerate(f_src.readlines()):
            likes_producer.send(likes_topic, line)
            print(f"\rSent {i + 1}", end=str())
    ```

4. *posts*
    ```jupyterpython
    #%%
    ! head -{row_count} ~/shared-data/clickhouse_data/posts.json > ~/sample_posts.json
    #%%
    posts_producer = KafkaProducer(
        bootstrap_servers="kafka-svc:9092", 
        value_serializer=str.encode
    )
    posts_topic = "topic-posts"
    #%%
    with open("sample_posts.json") as f_src:
        for i, line in enumerate(f_src.readlines()):
            posts_producer.send(posts_topic, line)
            print(f"\rSent {i + 1}", end=str())
    ```

5. *users_profiles*
    ```jupyterpython
    #%%
    ! head -{row_count} ~/shared-data/clickhouse_data/users_profiles.json > ~/sample_users_profiles.json
    #%%
    user_profiles_producer = KafkaProducer(
        bootstrap_servers="kafka-svc:9092", 
        value_serializer=str.encode
    )
    user_profiles_topic = "topic-user-profiles"
    #%%
    with open("sample_user_profiles.json") as f_src:
        for i, line in enumerate(f_src.readlines()):
            user_profiles_producer.send(user_profiles_topic, line)
            print(f"\rSent {i + 1}", end=str())
    ```
   
---

**Additional materialized views**

1. *number of likes*
    ```clickhouse
    CREATE MATERIALIZED VIEW sultan_db.mv_count_likes ON CLUSTER kube_clickhouse_cluster
    ENGINE = AggregatingMergeTree()
    ORDER BY (owner_id)
    POPULATE
    AS SELECT
        owner_id,
        countState() AS likes_count
    FROM sultan_db.likes
    GROUP BY owner_id;
    
    CREATE TABLE sultan_db.distr_count_likes ON CLUSTER kube_clickhouse_cluster 
    AS sultan_db.mv_count_likes
    ENGINE = Distributed(kube_clickhouse_cluster, sultan_db, mv_count_likes);
    ```

2. *number of followers*
    ```clickhouse
    CREATE MATERIALIZED VIEW sultan_db.mv_count_followers ON CLUSTER kube_clickhouse_cluster   
    ENGINE = AggregatingMergeTree() 
    ORDER BY (id, first_name)
    POPULATE
    AS SELECT
        id,
        first_name,
        countState() AS followers_count
    FROM sultan_db.followers as f
    INNER JOIN sultan_db.user_profiles as up
    ON up.id = f.profile_id
    GROUP BY id, first_name;
   
    CREATE TABLE sultan_db.distr_count_followers ON CLUSTER kube_clickhouse_cluster 
    AS sultan_db.mv_count_followers
    ENGINE = Distributed(kube_clickhouse_cluster, sultan_db, mv_count_followers);
    ```
