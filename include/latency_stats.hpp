#pragma once
#include <vector>
#include <cstdint>
#include <algorithm>
#include <cstdio>

namespace mdfeed {

struct PercentileReport {
    double p50_ns, p90_ns, p99_ns, p999_ns, max_ns, mean_ns;
    size_t n;
};

inline PercentileReport percentiles(std::vector<uint64_t>& samples) {
    PercentileReport r{};
    r.n = samples.size();
    if (samples.empty()) return r;
    std::sort(samples.begin(), samples.end());
    auto at = [&](double p) -> double {
        size_t idx = static_cast<size_t>(p * (samples.size() - 1));
        return static_cast<double>(samples[idx]);
    };
    r.p50_ns = at(0.50);
    r.p90_ns = at(0.90);
    r.p99_ns = at(0.99);
    r.p999_ns = at(0.999);
    r.max_ns = static_cast<double>(samples.back());
    double sum = 0;
    for (auto v : samples) sum += static_cast<double>(v);
    r.mean_ns = sum / samples.size();
    return r;
}

inline void print_report(const char* label, const PercentileReport& r) {
    std::printf("%-28s n=%-10zu mean=%8.0fns  p50=%8.0fns  p90=%8.0fns  p99=%8.0fns  p99.9=%9.0fns  max=%10.0fns\n",
                label, r.n, r.mean_ns, r.p50_ns, r.p90_ns, r.p99_ns, r.p999_ns, r.max_ns);
}

} // namespace mdfeed
