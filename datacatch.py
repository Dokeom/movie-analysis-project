import requests
import pandas as pd
import time
from tqdm import tqdm  # 进度条工具（可选安装）

# ------------------------------
# 配置区（需要用户修改）
# ------------------------------
API_KEY = "YOUR_TMDB_API_KEY"  # 替换为你的API密钥
LANGUAGE = "zh-CN"             # 返回中文数据
SAVE_PATH = "movies_data.csv"  # 数据保存路径

# ------------------------------
# 功能函数：获取单部电影完整数据
# ------------------------------
def get_movie_data(movie_id):
    """
    通过TMDB API获取电影完整数据
    返回：包含电影信息的字典，若失败返回None
    """
    base_url = "https://api.themoviedb.org/3/movie"
    
    try:
        # 请求电影基础信息+演职员+社交媒体数据（合并请求减少API调用）
        response = requests.get(
            f"{base_url}/{movie_id}",
            params={
                "api_key": API_KEY,
                "language": LANGUAGE,
                "append_to_response": "credits,external_ids"
            },
            timeout=10
        )
        response.raise_for_status()  # 自动抛出HTTP错误
        
        data = response.json()
        
        # 提取关键数据
        movie_info = {
            "id": data.get("id"),
            "title": data.get("title"),
            "release_date": data.get("release_date"),
            "genres": ", ".join([g["name"] for g in data.get("genres", [])]),
            "vote_average": data.get("vote_average"),  # 用户评分
            "popularity": data.get("popularity"),      # 热度指标
            "director": extract_director(data.get("credits", {})),
            "actors": extract_top_actors(data.get("credits", {})),
            "imdb_id": data.get("external_ids", {}).get("imdb_id"),
            "facebook_id": data.get("external_ids", {}).get("facebook_id"),
            "instagram_id": data.get("external_ids", {}).get("instagram_id"),
            "twitter_id": data.get("external_ids", {}).get("twitter_id")
        }
        return movie_info
        
    except requests.exceptions.RequestException as e:
        print(f"电影ID {movie_id} 请求失败: {str(e)}")
        return None
    except KeyError as e:
        print(f"电影ID {movie_id} 数据解析错误: {str(e)}")
        return None

# ------------------------------
# 辅助函数：处理导演和演员信息
# ------------------------------
def extract_director(credits_data):
    """从credits数据中提取导演"""
    crew = credits_data.get("crew", [])
    directors = [m["name"] for m in crew if m.get("job") == "Director"]
    return ", ".join(directors[:3])  # 最多取前3位导演

def extract_top_actors(credits_data):
    """提取前5名主演"""
    cast = credits_data.get("cast", [])
    return ", ".join([m["name"] for m in cast[:5]])

# ------------------------------
# 主程序：批量获取电影数据
# ------------------------------
def main(start_id=1, end_id=100):
    all_movies = []
    
    # 使用进度条（需要安装tqdm库）
    for movie_id in tqdm(range(start_id, end_id + 1)):
        # 调用API获取数据
        movie_data = get_movie_data(movie_id)
        
        # 如果获取成功且包含必要字段
        if movie_data and movie_data.get("release_date"):
            all_movies.append(movie_data)
        
        # 遵守API速率限制（每秒3次）
        time.sleep(0.4)  # 间隔400ms
        
    # 保存到CSV
    if all_movies:
        df = pd.DataFrame(all_movies)
        df.to_csv(SAVE_PATH, index=False, encoding="utf-8-sig")
        print(f"成功保存 {len(all_movies)} 部电影数据到 {SAVE_PATH}")
    else:
        print("未获取到有效数据")

# ------------------------------
# 执行程序
# ------------------------------
if __name__ == "__main__":
    # 示例：爬取ID 1-100的电影（实际ID不连续，建议改用搜索功能）
    main(start_id=1, end_id=100)