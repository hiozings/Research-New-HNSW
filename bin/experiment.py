#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import subprocess, time, os, json, shutil
import argparse, requests, matplotlib.pyplot as plt

def start_process(path, args):
    """启动子进程"""
    return subprocess.Popen([path] + args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def get_hnsw_mem(port=8080):
    """调用 /mem 接口获取 hnsw_service 的 RSS（MB）"""
    try:
        resp = requests.get(f"http://127.0.0.1:{port}/mem", timeout=3)
        if resp.ok:
            return resp.json().get("rss_kb", 0.0) / 1024.0
    except Exception as e:
        print("Failed to get /mem:", e)
    return 0.0

def clean_data(dbpath='./rocksdb_data'):
    """删除旧的 RocksDB 数据与图文件"""
    if os.path.exists(dbpath):
        shutil.rmtree(dbpath)
    if os.path.exists('./hnsw_graph.bin'):
        os.remove('./hnsw_graph.bin')

def run_experiment(sizes, dim=128, dbpath='./rocksdb_data', opt_mode=False, dpi=200, n_search=20, M=16, ef_construction=200):
    results = []
    mode = "optimized" if opt_mode else "baseline"
    os.makedirs('res', exist_ok=True)
    print(f"\n==== Running {mode} mode ====\n")

    for N in sizes:
        print(f"[INFO] Building index N={N}  (mode={mode})")

        # 清理旧数据
        clean_data(dbpath)
        time.sleep(0.5)

        # 构建索引
        proc = subprocess.run('./index_builder', [str(N), str(dim), dbpath, './hnsw_graph.bin',
                              str(M), str(ef_construction)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if proc.returncode != 0:
            print("index_builder failed with return code", proc.returncode)
            return

        # 启动 storage_service
        storage = start_process('./storage_service', [dbpath, '8081'])
        time.sleep(1.0)

        # 启动 hnsw_service
        hnsw = start_process(
            './hnsw_service',
            ['--graph', './hnsw_graph.bin',
            '--storage', 'http://127.0.0.1:8081',
            '--port', '8080',
            '--ef', '200',
            '--optimized', str(int(opt_mode)),
            '--dim', str(dim)]
        )
        time.sleep(1.5)

        # 等待 /mem 可访问
        for _ in range(10):
            if get_hnsw_mem() > 0:
                break
            time.sleep(0.5)

        # 实时测量搜索过程内存
        print(f"[INFO] Starting {n_search} search requests to measure memory...")
        mem_trace = []
        query = {"query": [0.1] * dim, "k": 10, 'ef': 200}

        for i in range(n_search):
            try:
                # 发出一次 /search
                resp = requests.post("http://127.0.0.1:8080/search", json=query, timeout=5)
                if resp.ok:
                    res = resp.json().get('results', [])
                    print(f"Search {i+1}/{n_search}: got {len(res)} results")
            except Exception as e:
                print(f"Search {i+1} failed:", e)

            # 记录当前RSS
            rss = get_hnsw_mem()
            mem_trace.append(rss)
            time.sleep(0.2)

        # 计算统计数据
        avg_mem = sum(mem_trace) / len(mem_trace)
        peak_mem = max(mem_trace)
        results.append({'N': N, 'avg_rss': avg_mem, 'peak_rss': peak_mem})
        print(f"[RESULT] N={N} avg={avg_mem:.1f}MB peak={peak_mem:.1f}MB")

        # 绘制每轮内存曲线
        plt.figure(figsize=(6,4))
        plt.plot(mem_trace, label=f'N={N}')
        plt.xlabel('Search Iteration')
        plt.ylabel('RSS (MB)')
        plt.title(f'HNSW {mode} N={N}')
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(f'res/hnsw_mem_trace_{mode}_N{N}.png', dpi=dpi)
        plt.close()

        # 关闭服务
        hnsw.terminate()
        storage.terminate()
        time.sleep(1.0)

    # 汇总绘图
    Ns = [r['N'] for r in results]
    avg_vals = [r['avg_rss'] for r in results]
    peak_vals = [r['peak_rss'] for r in results]

    plt.figure(figsize=(6,4))
    plt.plot(Ns, avg_vals, marker='o', label='Average RSS')
    plt.plot(Ns, peak_vals, marker='x', label='Peak RSS')
    plt.xlabel('N')
    plt.ylabel('RSS (MB)')
    plt.title(f'HNSW {mode} Memory Usage')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(f'res/memory_usage_{mode}.png', dpi=dpi)
    plt.close()

    # 保存结果
    with open(f'res/results_{mode}.json', 'w') as f:
        json.dump(results, f, indent=2)
    print(f"\n✅ {mode} experiment finished. Results saved to res/results_{mode}.json and res/memory_usage_{mode}.png")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--sizes', nargs='+', type=int, default=[10000, 50000, 100000])
    parser.add_argument('--dim', type=int, default=128)
    parser.add_argument('--opt', action='store_true', help='also run optimized mode test')
    parser.add_argument('--dpi', type=int, default=200)
    parser.add_argument('--n_search', type=int, default=20, help='number of /search requests per test')
    parser.add_argument('--M', type=int, default=16, help='max neighbors per node')
    parser.add_argument('--ef_construction', type=int, default=200, help='ef_construction parameter')
    args = parser.parse_args()

    # 非优化模式
    run_experiment(args.sizes, args.dim, opt_mode=False, dpi=args.dpi, n_search=args.n_search, M=args.M, ef_construction=args.ef_construction)
    # 优化模式
    if args.opt:
        run_experiment(args.sizes, args.dim, opt_mode=True, dpi=args.dpi, n_search=args.n_search, M=args.M, ef_construction=args.ef_construction)




    