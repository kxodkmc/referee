//! A realistic host program that drives the `socstat` SDK end-to-end.
//!
//! Simulates how an embedder would load a CSV, run a full statistical
//! workflow (descriptive → frequencies → crosstab → hypothesis tests →
//! regression → logistic → PCA → reliability), and persist results as JSON.
//!
//! Run with: `cargo run --example host_demo`

use socstat::prelude::*;
use socstat_verify::survey;

fn main() -> SocStatResult<()> {
    // ------------------------------------------------------------------
    // 1. Build a dataset and persist/reload it through a CSV file.
    // ------------------------------------------------------------------
    let mut ds = survey()?;
    ds.set_weight("w")?;
    let csv_path = std::env::temp_dir().join("socstat_host_demo.csv");
    socstat::write().csv(&ds, &csv_path)?;
    let ds = socstat::read().csv(&csv_path)?;
    println!("Host: loaded {} vars × {} rows from {}", ds.n_vars(), ds.n_rows(), csv_path.display());

    // ------------------------------------------------------------------
    // 2. Descriptive statistics + frequencies + crosstab.
    // ------------------------------------------------------------------
    let d = ds.descriptive("score")?;
    println!("\n== Descriptive: score ==");
    println!("  n={:.0} mean={:.2} sd={:.2} median={:.1}", d.n, d.mean, d.std_dev, d.median);
    println!("  min={:.1} max={:.1} Q1={:.1} Q3={:.1} skew={:.3} kurt={:.3}", d.min, d.max, d.q1, d.q3, d.skewness, d.kurtosis);
    println!("  95% CI = [{:.2}, {:.2}]", d.ci_95.0, d.ci_95.1);

    let f = ds.frequencies("group")?;
    println!("\n== Frequencies: group ==");
    for row in f.iter() {
        println!("  {:<8} n={:<3} {:.1}% (valid {:.1}%, cum {:.1}%)", row.value, row.count, row.percent, row.valid_percent, row.cumulative);
    }
    println!("  valid={} missing={}", f.n_valid, f.n_missing);

    let ct = ds.crosstab("group", "outcome")?;
    println!("\n== Crosstab: group × outcome ==");
    print!("  {:8}", "");
    for cl in &ct.col_labels { print!(" {:>8}", cl); }
    println!(" {:>8}", "Total");
    for (i, rl) in ct.row_labels.iter().enumerate() {
        print!("  {:8}", rl);
        for &c in &ct.counts[i] { print!(" {:>8}", c); }
        println!(" {:>8}", ct.row_totals[i]);
    }

    // ------------------------------------------------------------------
    // 3. Hypothesis tests.
    // ------------------------------------------------------------------
    let t = ds.independent_t_test("score", "group")?;
    println!("\n== Independent t-test: score by group ==");
    for g in &t.group_stats { println!("  {} n={:.0} mean={:.3} sd={:.3}", g.label, g.n, g.mean, g.std_dev); }
    println!("  Levene F={:.4} p={:.4}", t.levene_test.f_statistic, t.levene_test.p_value);
    println!("  pooled t={:.4} df={:.1} p={:.4} | Welch t={:.4} df={:.1} p={:.4}",
        t.equal_variances.t_statistic, t.equal_variances.df, t.equal_variances.p_value,
        t.unequal_variances.t_statistic, t.unequal_variances.df, t.unequal_variances.p_value);

    let a = ds.one_way_anova("score", "dose")?;
    println!("\n== One-way ANOVA: score by dose ==");
    println!("  F({:.0},{:.0}) = {:.4} p={:.4} η²={:.4}", a.between_groups.df, a.within_groups.df, a.f_statistic, a.p_value, a.eta_squared);

    let c = ds.chi_square_test("group", "outcome")?;
    println!("\n== Chi-square: group × outcome ==");
    println!("  χ²({:.0}) = {:.4} p={:.4} n={:.0}", c.df, c.chi_square, c.p_value, c.n);

    let fe = ds.fisher_exact_test("group", "outcome", Alternative::TwoSided)?;
    println!("  Fisher exact OR={:.3} p2={:.4}", fe.odds_ratio, fe.p_value_two_sided);

    let an = ds.paired_t_test("pre", "post")?;
    println!("\n== Paired t-test: pre vs post ==");
    println!("  d̄={:.3} t={:.4} df={:.0} p={:.4} r_paired={:.3}", an.mean_difference, an.t_statistic, an.df, an.p_value, an.correlation);

    let m = ds.mann_whitney_u_test("score", "group")?;
    println!("\n== Mann-Whitney U: score by group ==");
    println!("  U={:.1} z={:.4} p={:.4} ties={}", m.u_statistic, m.z_score, m.p_value, m.has_ties);

    let kw = ds.kruskal_wallis_test("score", "dose")?;
    println!("  Kruskal-Wallis H={:.4} df={:.0} p={:.4}", kw.h_statistic, kw.df, kw.p_value);

    // ------------------------------------------------------------------
    // 4. Normality + post-hoc.
    // ------------------------------------------------------------------
    let sw = ds.shapiro_wilk("score")?;
    println!("\n== Normality: score ==");
    println!("  Shapiro-Wilk W={:.4} p={:.4}", sw.w_statistic, sw.p_value);

    let ph = ds.post_hoc("score", "dose", PostHocMethod::Tukey)?;
    println!("\n== Tukey HSD: score by dose ==");
    for cmp in &ph.comparisons {
        println!("  {} vs {}: Δ={:.3} p={:.4} CI=[{:.3},{:.3}]", cmp.group1, cmp.group2, cmp.mean_difference, cmp.p_value, cmp.ci_95.0, cmp.ci_95.1);
    }

    let fa = ds.factorial_anova("score", &["group", "dose"], SsType::TypeII)?;
    println!("\n== Factorial ANOVA (Type II): score ~ group × dose ==");
    for eff in &fa.effects {
        if let (Some(f_), Some(p)) = (eff.f, eff.p_value) {
            println!("  {}: F={:.3} p={:.4}", eff.source, f_, p);
        }
    }

    // ------------------------------------------------------------------
    // 5. Correlation + regression.
    // ------------------------------------------------------------------
    let r = ds.correlation_pair("income", "spend", CorrelationMethod::Pearson)?;
    let rc = r.coefficient().unwrap();
    println!("\n== Correlation: income ~ spend ==");
    println!("  r={:.4} p={:.4} n={:.0}", rc.coefficient, rc.p_value, r.n);

    let model = ds.regression("spend", &["income", "score"])?;
    println!("\n== Linear regression: spend ~ income + score ==");
    println!("  R²={:.4} adjR²={:.4} F={:.3} p={:.4}", model.r_squared, model.adj_r_squared, model.f_statistic, model.f_p_value);
    for coef in &model.coefficients {
        println!("  {:10} β={:8.4} se={:.4} t={:.3} p={:.4}", coef.name, coef.estimate, coef.std_error, coef.t_statistic, coef.p_value);
    }

    let vifs = ds.vif(&["income", "score", "q1"])?;
    println!("\n== VIF ==");
    for v in &vifs { println!("  {:8} VIF={:.3} tol={:.3}", v.variable, v.vif, v.tolerance); }

    // ------------------------------------------------------------------
    // 6. Logistic regression.
    // ------------------------------------------------------------------
    let logi = ds.logistic_regression("binary", &["income", "score"])?;
    println!("\n== Logistic regression: binary ~ income + score ==");
    println!("  AIC={:.2} deviance={:.2} converged={}", logi.aic, logi.residual_deviance, logi.converged);
    for coef in &logi.coefficients {
        println!("  {:10} β={:8.4} OR={:.3} se={:.4} z={:.3} p={:.4}", coef.name, coef.estimate, coef.odds_ratio, coef.std_error, coef.z_statistic, coef.p_value);
    }
    let cm = logi.confusion_matrix(&ds, 0.5)?;
    println!("  Confusion: accuracy={:.3} precision={:.3} recall={:.3} f1={:.3}", cm.accuracy, cm.precision, cm.recall, cm.f1);

    // ------------------------------------------------------------------
    // 7. Multivariate: PCA + reliability.
    // ------------------------------------------------------------------
    let pca = ds.pca(&["q1", "q2", "q3"], PcaMatrix::Correlation)?;
    println!("\n== PCA (Correlation) on q1..q3 ==");
    for (i, comp) in pca.components.iter().enumerate() {
        println!("  PC{} λ={:.3} explained={:.1}% cumulative={:.1}%", i + 1, comp.eigenvalue, comp.explained_variance_ratio * 100.0, comp.cumulative_variance_ratio * 100.0);
    }

    let rel = ds.reliability(&["q1", "q2", "q3"])?;
    println!("\n== Reliability: α = {:.4} (standardized {:.4}) n_cases={} ==", rel.alpha, rel.standardized_alpha, rel.n_cases);
    for it in &rel.item_statistics {
        println!("  {}: corrected r={:.3} α-if-deleted={:.3}", it.item, it.corrected_item_total_correlation, it.alpha_if_deleted);
    }

    // ------------------------------------------------------------------
    // 8. Persist a result as JSON (the shape hosts receive).
    // ------------------------------------------------------------------
    let out = std::env::temp_dir().join("socstat_host_demo_score.json");
    let json = serde_json::to_string_pretty(&d).unwrap();
    std::fs::write(&out, json).map_err(|e| SocStatError::Other(e.to_string()))?;
    println!("\nHost: wrote Descriptive JSON to {}", out.display());

    println!("\nHost: full chain complete ✅");
    Ok(())
}