//! Full-module integration tests for the `socstat` SDK, run from a host crate.
//!
//! Builds realistic datasets via `socstat_verify::survey` and drives every
//! public analysis path through the `StatsExt` trait plus data transforms and
//! I/O round-trips — the same shape a host application consumes.

use socstat::prelude::*;
use socstat_verify::survey;

#[test]
fn data_model_and_transforms() -> SocStatResult<()> {
    let mut ds = survey()?;
    assert_eq!(ds.n_vars(), 13);
    assert_eq!(ds.n_rows(), 20);

    ds.compute("bmi_like", |row| row.numeric("score").map(|s| s / 100.0))?;
    assert_eq!(ds.numeric_values("bmi_like")?.len(), 20);

    ds.compute_text("arm_label", |row| {
        row.text("group")
            .map(|g| if g == "A" { "arm_a" } else { "arm_b" }.to_string())
    })?;
    assert_eq!(ds.text_slice("arm_label")?.len(), 20);

    // recode maps a numeric variable's values (closure over Option<f64>).
    ds.recode("score", |v| v.map(|x| if x >= 85.0 { 2.0 } else { 1.0 }))?;
    assert_eq!(ds.n_valid("score")?, 20);

    // recode_into writes a new variable, keeping the source column intact.
    ds.recode_into("score", "score_bin", |v| v.map(|x| (x >= 2.0) as u8 as f64))?;
    assert_eq!(ds.n_valid("score_bin")?, 20);

    let kept = ds.filter(|row| row.numeric("income") > Some(1800.0))?;
    assert!(kept > 0 && kept < 20);

    ds.sort_by("income", true)?;
    let v = ds.numeric_values("income")?;
    assert!(v.windows(2).all(|w| w[0] >= w[1]));

    ds.keep(&["score", "income", "group"])?;
    assert_eq!(ds.n_vars(), 3);
    Ok(())
}

#[test]
fn descriptive_frequencies_crosstab() -> SocStatResult<()> {
    let ds = survey()?;
    let d = ds.descriptive("score")?;
    assert_eq!(d.n, 20.0);
    assert!((83.0..86.0).contains(&d.mean));
    assert!(d.std_dev > 0.0 && d.median > 0.0 && d.ci_95.0 < d.ci_95.1);

    let f = ds.frequencies("group")?;
    assert_eq!(f.n_valid, 20);
    assert_eq!(f.n_missing, 0);
    assert_eq!(f.iter().count(), 2);
    let total: f64 = f.iter().map(|r| r.percent).sum();
    assert!((total - 100.0).abs() < 1e-6);

    let ct = ds.crosstab("group", "outcome")?;
    assert_eq!(ct.row_labels.len(), 2);
    assert_eq!(ct.col_labels.len(), 2);
    assert_eq!(ct.counts.len(), 2);
    Ok(())
}

#[test]
fn hypothesis_tests() -> SocStatResult<()> {
    let ds = survey()?;

    let t = ds.independent_t_test("score", "group")?;
    assert_eq!(t.group_stats.len(), 2);
    assert!(t.equal_variances.p_value.is_finite());
    assert!(t.unequal_variances.t_statistic.is_finite());

    let a = ds.one_way_anova("score", "dose")?;
    assert_eq!(a.group_stats.len(), 3);
    assert!(a.between_groups.df > 0.0 && a.within_groups.df > 0.0);
    assert!(a.f_statistic.is_finite() && a.p_value.is_finite());
    assert!(a.eta_squared >= 0.0);

    let c = ds.chi_square_test("group", "outcome")?;
    assert!(c.chi_square >= 0.0 && c.p_value.is_finite());

    let m = ds.mann_whitney_u_test("score", "group")?;
    assert_eq!(m.group_stats.len(), 2);
    assert!(m.p_value.is_finite());

    let p = ds.paired_t_test("pre", "post")?;
    assert_eq!(p.n, 20.0);
    // dᵢ = v1 − v2 = pre − post, so pre < post ⇒ negative mean difference.
    assert!(p.mean_difference.is_finite() && p.mean_difference < 0.0);
    assert!(p.p_value.is_finite());

    let w = ds.wilcoxon_signed_rank_test("pre", "post")?;
    assert!(w.p_value.is_finite());

    let kw = ds.kruskal_wallis_test("score", "dose")?;
    assert_eq!(kw.group_stats.len(), 3);
    assert!(kw.h_statistic.is_finite() && kw.p_value.is_finite());
    Ok(())
}

#[test]
fn fisher_posthoc_factorial() -> SocStatResult<()> {
    let ds = survey()?;

    let fe = ds.fisher_exact_test("group", "outcome", Alternative::TwoSided)?;
    assert!(fe.odds_ratio.is_finite());
    assert!(fe.p_value_two_sided.is_finite());

    let ph = ds.post_hoc("score", "dose", PostHocMethod::Tukey)?;
    assert_eq!(ph.n_groups, 3);
    assert!(ph.comparisons.len() >= 3);
    assert!(ph.ms_within > 0.0 && ph.df_within > 0.0);

    let fa = ds.factorial_anova("score", &["group", "dose"], SsType::TypeII)?;
    assert_eq!(fa.factors.len(), 2);
    assert!(fa.effects.iter().all(|e| e.ss.is_finite()));
    assert!(fa.r_squared.is_finite());
    Ok(())
}

#[test]
fn normality_tests() -> SocStatResult<()> {
    let ds = survey()?;

    let sw = ds.shapiro_wilk("score")?;
    assert!(sw.w_statistic > 0.0 && sw.p_value.is_finite());

    let ks = ds.ks_normality_test("score", KsTestType::Lilliefors)?;
    assert!(ks.d_statistic >= 0.0 && ks.p_value.is_finite());

    let ks2 = ds.ks_normality_test("score", KsTestType::OneSample { mean: 80.0, std_dev: 7.0 })?;
    assert!(ks2.d_statistic >= 0.0);
    Ok(())
}

#[test]
fn correlation_and_partial() -> SocStatResult<()> {
    let ds = survey()?;

    let pair = ds.correlation_pair("income", "spend", CorrelationMethod::Pearson)?;
    assert_eq!(pair.var1, "income");
    assert_eq!(pair.var2, "spend");
    let r = pair.coefficient().expect("pearson populated");
    assert!(r.coefficient.is_finite() && r.p_value.is_finite());

    let all = ds.correlation(&["income", "spend", "score"], CorrelationMethod::Pearson)?;
    assert_eq!(all.len(), 3); // 3 choose 2 pairs

    let partial = ds.partial_correlation("income", "spend", &["score"], CorrelationMethod::Pearson)?;
    assert!(partial.coefficient.is_finite() && partial.df > 0.0);
    Ok(())
}

#[test]
fn linear_regression_and_vif() -> SocStatResult<()> {
    let ds = survey()?;
    let model = ds.regression("spend", &["income", "score"])?;
    assert!(model.r_squared > 0.5); // spend and income are strongly related
    assert_eq!(model.coefficients.len(), 3); // intercept + 2 predictors
    assert!(model.f_statistic.is_finite() && model.f_p_value.is_finite());

    assert_eq!(model.intercept(), model.coefficients[0].estimate);

    let pred = model.predict(&ds)?;
    assert_eq!(pred.len(), 20);
    assert!(pred.iter().all(|p| p.is_some()));

    let vifs = ds.vif(&["income", "score", "q1"])?;
    assert_eq!(vifs.len(), 3);
    assert!(vifs.iter().all(|v| v.vif > 0.0 && v.tolerance > 0.0));
    Ok(())
}

#[test]
fn logistic_regression_and_confusion() -> SocStatResult<()> {
    let ds = survey()?;
    let model = ds.logistic_regression("binary", &["income", "score"])?;
    assert!(model.converged);
    assert!(model.aic.is_finite() && model.residual_deviance.is_finite());
    assert!(model.mcfadden_r2.is_finite() && model.cox_snell_r2.is_finite());
    assert_eq!(model.coefficients.len(), 3);
    for c in &model.coefficients {
        assert!(c.odds_ratio > 0.0); // exp(beta) is always positive
        assert!(c.z_statistic.is_finite() && c.p_value.is_finite());
    }

    let cm = model.confusion_matrix(&ds, 0.5)?;
    assert!(cm.accuracy.is_finite() && cm.threshold == 0.5);
    Ok(())
}

#[test]
fn pca_and_reliability() -> SocStatResult<()> {
    let ds = survey()?;
    let pca = ds.pca(&["q1", "q2", "q3"], PcaMatrix::Correlation)?;
    // q1/q2/q3 are strongly collinear, so degenerate components (eigenvalue
    // below the noise floor) are dropped — at least one component survives.
    assert!(pca.components.len() >= 1);
    assert_eq!(pca.variables.len(), 3);
    let last = pca.components.last().unwrap();
    assert!((last.cumulative_variance_ratio - 1.0).abs() < 1e-6);
    let scores = pca.scores(&ds)?;
    assert_eq!(scores.nrows(), 20);

    let rel = ds.reliability(&["q1", "q2", "q3"])?;
    assert!(rel.alpha.is_finite() && rel.alpha > 0.0);
    assert_eq!(rel.item_statistics.len(), 3);
    for it in &rel.item_statistics {
        assert!(it.corrected_item_total_correlation.is_finite());
        assert!(it.alpha_if_deleted.is_finite());
    }
    Ok(())
}

#[test]
fn io_roundtrip_csv_json_sav() -> SocStatResult<()> {
    let mut ds = survey()?;
    ds.set_weight("w")?;
    let dir = std::env::temp_dir();

    let csv = dir.join("socstat_it.csv");
    socstat::write().csv(&ds, &csv)?;
    let ds_csv = socstat::read().csv(&csv)?;
    assert_eq!(ds_csv.n_rows(), 20);
    assert_eq!(ds_csv.n_vars(), 13);

    let json = dir.join("socstat_it.json");
    socstat::write().json(&ds, &json)?;
    let ds_json = socstat::read().json(&json)?;
    assert_eq!(ds_json.n_rows(), 20);
    assert_eq!(ds_json.n_vars(), 13);

    // SAV is available because this harness depends on socstat with `full`
    // (which includes the `sav` feature).
    let sav = dir.join("socstat_it.sav");
    socstat::write().sav(&ds, &sav)?;
    let ds_sav = socstat::read().sav(&sav)?;
    assert_eq!(ds_sav.n_rows(), 20);
    Ok(())
}

#[test]
fn results_are_serializable() -> SocStatResult<()> {
    let ds = survey()?;

    let d = ds.descriptive("score")?;
    let json = serde_json::to_string(&d).unwrap();
    let back: Descriptive = serde_json::from_str(&json).unwrap();
    assert_eq!(back.n, d.n);
    assert_eq!(back.mean, d.mean);

    let t = ds.independent_t_test("score", "group")?;
    let t_json = serde_json::to_string(&t).unwrap();
    let _back: IndependentTTest = serde_json::from_str(&t_json).unwrap();

    let model = ds.regression("spend", &["income"])?;
    let m_json = serde_json::to_string(&model).unwrap();
    let back: LinearRegressionResult = serde_json::from_str(&m_json).unwrap();
    assert_eq!(back.r_squared, model.r_squared);
    assert_eq!(back.coefficients.len(), 2);
    Ok(())
}