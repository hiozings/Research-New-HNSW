#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import subprocess, time, os, json, shutil
import matplotlib
matplotlib.use('Agg')
import argparse, requests, matplotlib.pyplot as plt
import random

def start_process(path, args):
    """启动子进程"""
    dev_null = open(os.devnull, 'w')
    return subprocess.Popen([path] + args, stdout=dev_null, stderr=dev_null, text=True)

def get_hnsw_mem(port=8080):
    """调用 /mem 接口获取 hnsw_service 的 RSS"""
    try:
        resp = requests.get(f"http://127.0.0.1:{port}/mem", timeout=20)
        if resp.ok:
            return resp.json().get("rss_kb", 0.0) / 1024.0
    except Exception as e:
        print("Failed to get /mem:", e)
    return 0.0

def clean_data(dbpath='./rocksdb_data'):
    """删除旧的数据"""
    if os.path.exists(dbpath):
        shutil.rmtree(dbpath)
    if os.path.exists('./hnsw_graph.bin'):
        os.remove('./hnsw_graph.bin')
    if os.path.exists('./hnsw_graph.bin.adj'):
        os.remove('./hnsw_graph.bin.adj')

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
        proc = subprocess.run(['./index_builder', str(N), str(dim), dbpath, './hnsw_graph.bin',
                              str(M), str(ef_construction)], text=True)
        if proc.returncode != 0:
            print("index_builder failed with return code", proc.returncode)
            return

        # 启动 storage_service
        storage = start_process('./storage_service', [dbpath, '8081'])
        time.sleep(1.0)


        graph_path = './hnsw_graph.bin'
        hnsw = start_process(
            './hnsw_service',
            ['--graph', graph_path,
            '--storage', 'http://127.0.0.1:8081',
            '--port', '8080',
            '--ef', '200',
            '--optimized', str(int(opt_mode)),
            '--dim', str(dim)]
        )
        time.sleep(1.5)

        #确保/mem接口正常
        for _ in range(10):
            if get_hnsw_mem() > 0:
                break
            time.sleep(0.5)

        # 测量搜索过程内存
        print(f"[INFO] Starting {n_search} search requests to measure memory...")
        mem_trace = []
        # query = {"query": [0.1] * dim, "k": 10, 'ef': 200}
        query = {
            "query": [random.uniform(-1.0, 1.0) for _ in range(dim)], 
            "k": 10, 
            'ef': 200
        }

        for i in range(n_search):
            if hnsw.poll() is not None:
                print(f"hnsw_service 在第 {i+1} 次搜索前已崩溃")
                break

            try:
                # 发出/search
                resp = requests.post("http://127.0.0.1:8080/search", json=query, timeout=120)
                if resp.ok:
                    res = resp.json().get('results', [])
                    print(f"Search {i+1}/{n_search}: got {len(res)} results")
                else:
                    print(f"Search {i+1}/{n_search}: HTTP {resp.status_code}")
            except requests.exceptions.Timeout:
                print(f"Search {i+1}/{n_search}: 超时，服务可能已崩溃")
                if hnsw.poll() is not None:
                    print("hnsw_service 已崩溃")
                    break
            except Exception as e:
                print(f"Search {i+1} failed:", e)

            # 记录当前RSS
            try:
                if hnsw.poll() is None:
                    rss = get_hnsw_mem()
                    mem_trace.append(rss)
                else:
                    mem_trace.append(0)
            except Exception as e:
                print(f"Failed to get memory usage:", e)

            time.sleep(0.2)

        # 计算统计数据
        avg_mem = sum(mem_trace) / len(mem_trace)
        peak_mem = max(mem_trace)
        results.append({'N': N, 'avg_rss': avg_mem, 'peak_rss': peak_mem})
        print(f"[RESULT] N={N} avg={avg_mem:.1f}MB peak={peak_mem:.1f}MB")

        # 绘制每轮内存曲线
        # plt.figure(figsize=(6,4))
        # plt.plot(mem_trace, label=f'N={N}')
        # plt.xlabel('Search Iteration')
        # plt.ylabel('RSS (MB)')
        # plt.title(f'HNSW {mode} N={N}')
        # plt.legend()
        # plt.grid(True)
        # plt.tight_layout()
        # plt.savefig(f'res/hnsw_mem_trace_{mode}_N{N}.png', dpi=dpi)
        # plt.close()

        # 关闭服务
        hnsw.terminate()
        storage.terminate()
        hnsw.wait()
        storage.wait()
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
    print(f"\n{mode} experiment finished. Results saved to res/results_{mode}.json and res/memory_usage_{mode}.png")

    return results

def calculate_memory_reduction(baseline_results, optimized_results, dpi=200):
    """计算内存下降比例"""
    baseline_dict = {r['N']: r for r in baseline_results}
    optimized_dict = {r['N']: r for r in optimized_results}
    
    # 确保N值集合一致
    common_Ns = sorted(set(baseline_dict.keys()) & set(optimized_dict.keys()))
    if not common_Ns:
        print("没有可对比的N值数据")
        return
    
    # 计算平均内存和峰值内存的下降比例
    avg_reductions = []
    peak_reductions = []
    
    for N in common_Ns:
        baseline = baseline_dict[N]
        optimized = optimized_dict[N]
        
        if baseline['avg_rss'] == 0:
            avg_reduction = 0.0
        else:
            avg_reduction = (baseline['avg_rss'] - optimized['avg_rss']) / baseline['avg_rss'] * 100
        
        if baseline['peak_rss'] == 0:
            peak_reduction = 0.0
        else:
            peak_reduction = (baseline['peak_rss'] - optimized['peak_rss']) / baseline['peak_rss'] * 100
        
        avg_reductions.append(avg_reduction)
        peak_reductions.append(peak_reduction)
        print(f"[REDUCTION] N={N} 平均内存下降: {avg_reduction:.1f}%  峰值内存下降: {peak_reduction:.1f}%")
    
    # 绘制内存下降比例图
    plt.figure(figsize=(6,4))
    plt.plot(common_Ns, avg_reductions, marker='o', label='Average Memory Reduction')
    plt.plot(common_Ns, peak_reductions, marker='x', label='Peak Memory Reduction')
    plt.xlabel('Vector Count (N)')
    plt.ylabel('Memory Reduction (%)')
    plt.title('HNSW Optimized Mode Memory Reduction')
    plt.ylim(0, 100)
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig('res/memory_reduction.png', dpi=dpi)
    plt.close()
    
    # 保存下降比例结果
    reduction_results = [{
        'N': N,
        'avg_reduction_pct': avg,
        'peak_reduction_pct': peak
    } for N, avg, peak in zip(common_Ns, avg_reductions, peak_reductions)]
    
    with open('res/memory_reduction.json', 'w') as f:
        json.dump(reduction_results, f, indent=2)
    print(f"\n内存下降比例计算完成，结果保存到 res/memory_reduction.png 和 res/memory_reduction.json")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--sizes', nargs='+', type=int, default=[10000, 50000, 100000])
    parser.add_argument('--dim', type=int, default=128)
    parser.add_argument('--opt', action='store_true', help='also run optimized mode test')
    parser.add_argument('--dpi', type=int, default=200)
    parser.add_argument('--n_search', type=int, default=10, help='number of /search requests per test')
    parser.add_argument('--M', type=int, default=16, help='max neighbors per node')
    parser.add_argument('--ef_construction', type=int, default=200, help='ef_construction parameter')
    args = parser.parse_args()

    # 非优化模式
    baseline_results = run_experiment(args.sizes, args.dim, opt_mode=False, dpi=args.dpi, n_search=args.n_search, M=args.M, ef_construction=args.ef_construction)
    # 优化模式
    if args.opt:
       optimized_results = run_experiment(args.sizes, args.dim, opt_mode=True, dpi=args.dpi, n_search=args.n_search, M=args.M, ef_construction=args.ef_construction)
       calculate_memory_reduction(baseline_results, optimized_results, args.dpi)




    