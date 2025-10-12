import argparse
import requests
import time
import matplotlib.pyplot as plt

# ========== 工具函数 ==========

def get_server_rss():
    """向 hnsw_service 请求当前内存（MB）"""
    try:
        r = requests.get("http://127.0.0.1:8080/mem", timeout=2)
        if r.status_code == 200:
            kb = r.json().get("rss_kb", 0)
            return kb / 1024.0  # 转换为 MB
    except Exception as e:
        print(f"[WARN] 无法获取RSS: {e}")
    return None


def send_search_query(dim=128):
    """随机生成一个查询向量并发送 /search 请求"""
    import random
    vec = [random.random() for _ in range(dim)]
    try:
        r = requests.post("http://127.0.0.1:8080/search",
                          json={"query": vec, "k": 10}, timeout=5)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print(f"[WARN] /search 请求失败: {e}")
    return None


def run_search_and_measure(dim=128, repeat=20):
    """多次触发搜索并获取峰值内存"""
    max_mem = 0.0
    for i in range(repeat):
        send_search_query(dim)
        time.sleep(0.1)
        mem = get_server_rss()
        if mem and mem > max_mem:
            max_mem = mem
    print(f"  🔍 搜索阶段最大RSS={max_mem:.1f}MB")
    return max_mem


def run_experiment(sizes, dim, optimized=False):
    """对不同 N 运行实验"""
    mode = "优化模式" if optimized else "普通模式"
    print(f"\n===== {mode} 测试开始 =====")

    results = []
    for N in sizes:
        print(f"\n🧩 N = {N}")
        # 向 storage_service 创建数据
        try:
            r = requests.post("http://127.0.0.1:8090/generate",
                              json={"N": N, "dim": dim}, timeout=10)
            assert r.status_code == 200
            print("  ✅ 数据生成完成")
        except Exception as e:
            print(f"  ❌ 数据生成失败: {e}")
            continue

        # 通知 hnsw_service 载入数据
        try:
            r = requests.post("http://127.0.0.1:8080/load",
                              json={"opt": optimized}, timeout=30)
            assert r.status_code == 200
            print("  ✅ 图加载完成")
        except Exception as e:
            print(f"  ❌ 图加载失败: {e}")
            continue

        # 执行搜索阶段并测量峰值内存
        mem_peak = run_search_and_measure(dim)
        results.append({
            "N": N,
            "mem_peak": mem_peak
        })

    return results


def plot_results(results_normal, results_opt):
    """绘制两种模式的对比图"""
    plt.figure(figsize=(8,5))
    Ns = [r["N"] for r in results_normal]
    mem_normal = [r["mem_peak"] for r in results_normal]
    mem_opt = [r["mem_peak"] for r in results_opt]

    plt.plot(Ns, mem_normal, marker='o', label='普通模式')
    plt.plot(Ns, mem_opt, marker='s', label='优化模式')
    plt.xlabel("节点数 N")
    plt.ylabel("搜索阶段最大RSS内存 (MB)")
    plt.title("HNSW 搜索阶段内存占用对比")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    # plt.show()
    plt.savefig("res/memory_compare.png")
    print("\n✅ 实验完成：已生成 res/memory_compare.png")


# ========== 主程序入口 ==========

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sizes", nargs="+", type=int, default=[10000, 50000, 100000],
                        help="测试的节点数量列表")
    parser.add_argument("--dim", type=int, default=128, help="向量维度")
    parser.add_argument("--opt", action="store_true", help="同时测试优化与非优化模式")
    parser.add_argument("--repeat", type=int, default=20, help="每轮搜索次数")
    args = parser.parse_args()

    # 非优化模式
    results_normal = run_experiment(args.sizes, args.dim, optimized=False)

    # 优化模式（若启用）
    results_opt = []
    if args.opt:
        results_opt = run_experiment(args.sizes, args.dim, optimized=True)
        plot_results(results_normal, results_opt)
    else:
        print("\n结果（非优化模式）:")
        for r in results_normal:
            print(f"N={r['N']}, 最大RSS={r['mem_peak']:.1f}MB")
