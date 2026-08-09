param(
    [string]$ProjectFolder = "C:\Users\ishadave\OneDrive - Microsoft\Documents\Personal\NYU Cyber Fellows\Classes\Summer 26\Big Data\Juypter Hub Shared\Project",
    [string]$RepoRoot = (Split-Path -Parent $PSScriptRoot),
    [string]$OutputName = "CineScope_Final_Presentation_2026-08-10.pptx"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-Rgb([int]$Red, [int]$Green, [int]$Blue) {
    return $Red + (256 * $Green) + (65536 * $Blue)
}

$Color = @{
    Ink = Get-Rgb 24 31 36
    Paper = Get-Rgb 247 246 242
    White = Get-Rgb 255 255 255
    Teal = Get-Rgb 13 112 111
    TealDark = Get-Rgb 8 71 76
    Coral = Get-Rgb 220 91 73
    Gold = Get-Rgb 224 171 69
    Mist = Get-Rgb 226 234 232
    Gray = Get-Rgb 98 107 112
    LightGray = Get-Rgb 224 226 225
}

$MetricsDir = Join-Path $ProjectFolder "outputs\metrics"
$ChartsDir = Join-Path $ProjectFolder "outputs\charts\generated"
$OutputPath = Join-Path $ProjectFolder $OutputName

function Read-Version2Json([string]$Path) {
    if (-not (Test-Path $Path)) {
        return $null
    }
    $value = Get-Content $Path -Raw | ConvertFrom-Json
    if (-not $value.PSObject.Properties["schema_version"] -or $value.schema_version -ne 2) {
        return $null
    }
    return $value
}

function Format-Metric($Value, [int]$Digits = 3) {
    if ($null -eq $Value) {
        return "PENDING V2 RERUN"
    }
    return ([double]$Value).ToString("F$Digits")
}

$Analytics = Read-Version2Json (Join-Path $MetricsDir "analytics_metrics.json")
$Hit = Read-Version2Json (Join-Path $MetricsDir "hit_model_metrics.json")
$Awards = Read-Version2Json (Join-Path $MetricsDir "awards_model_metrics.json")
$HitPrCurve = $(if ($null -ne $Hit) { Join-Path $ChartsDir "hit_model_pr_curve.png" } else { "" })
$AwardsPrCurve = $(if ($null -ne $Awards) { Join-Path $ChartsDir "awards_model_pr_curve.png" } else { "" })
$HitConfusion = $(if ($null -ne $Hit) { Join-Path $ChartsDir "hit_model_confusion_metrics.png" } else { "" })
$AwardsConfusion = $(if ($null -ne $Awards) { Join-Path $ChartsDir "awards_model_confusion_metrics.png" } else { "" })
$GenreChart = $(if ($null -ne $Analytics) { Join-Path $ChartsDir "genre_decade_median_rating.png" } else { "" })
$RuntimeChart = $(if ($null -ne $Analytics) { Join-Path $ChartsDir "runtime_profile.png" } else { "" })
$DirectorChart = $(if ($null -ne $Analytics) { Join-Path $ChartsDir "director_prior_vs_rating.png" } else { "" })
$SignalLiftChart = $(if ($null -ne $Analytics) { Join-Path $ChartsDir "pre_release_signal_lift.png" } else { "" })
$SensitivityChart = $(if ($null -ne $Analytics) { Join-Path $ChartsDir "hit_label_sensitivity.png" } else { "" })

$PowerPoint = $null
$Presentation = $null

function Add-Text {
    param(
        $Slide,
        [string]$Text,
        [double]$X,
        [double]$Y,
        [double]$Width,
        [double]$Height,
        [double]$Size = 20,
        [int]$TextColor = $Color.Ink,
        [string]$Font = "Aptos",
        [bool]$Bold = $false,
        [int]$Align = 1
    )
    $shape = $Slide.Shapes.AddTextbox(1, $X, $Y, $Width, $Height)
    $shape.TextFrame.MarginLeft = 0
    $shape.TextFrame.MarginRight = 0
    $shape.TextFrame.MarginTop = 0
    $shape.TextFrame.MarginBottom = 0
    $shape.TextFrame.WordWrap = -1
    $range = $shape.TextFrame.TextRange
    $range.Text = $Text
    $range.Font.Name = $Font
    $range.Font.Size = $Size
    $range.Font.Color.RGB = $TextColor
    $range.Font.Bold = $(if ($Bold) { -1 } else { 0 })
    $range.ParagraphFormat.Alignment = $Align
    return $shape
}

function Add-Panel {
    param(
        $Slide,
        [double]$X,
        [double]$Y,
        [double]$Width,
        [double]$Height,
        [int]$FillColor = $Color.White,
        [int]$LineColor = $Color.LightGray
    )
    $shape = $Slide.Shapes.AddShape(5, $X, $Y, $Width, $Height)
    $shape.Fill.ForeColor.RGB = $FillColor
    $shape.Fill.Solid()
    $shape.Line.ForeColor.RGB = $LineColor
    $shape.Line.Weight = 1
    return $shape
}

function Add-Bullets {
    param(
        $Slide,
        [string[]]$Items,
        [double]$X,
        [double]$Y,
        [double]$Width,
        [double]$Size = 18,
        [double]$Gap = 42,
        [int]$TextColor = $Color.Ink,
        [int]$BulletColor = $Color.Coral
    )
    for ($index = 0; $index -lt $Items.Count; $index++) {
        $top = $Y + ($index * $Gap)
        $marker = $Slide.Shapes.AddShape(1, $X, $top + 8, 8, 8)
        $marker.Fill.ForeColor.RGB = $BulletColor
        $marker.Fill.Solid()
        $marker.Line.Visible = 0
        Add-Text $Slide $Items[$index] ($X + 18) $top ($Width - 18) ($Gap - 2) $Size $TextColor | Out-Null
    }
}

function Add-Title {
    param($Slide, [string]$Number, [string]$Title, [bool]$Dark = $false)
    $textColor = $(if ($Dark) { $Color.White } else { $Color.Ink })
    Add-Text $Slide $Number 48 30 46 28 13 $Color.Coral "Bahnschrift" $true | Out-Null
    Add-Text $Slide $Title 96 24 800 46 27 $textColor "Bahnschrift SemiBold" $true | Out-Null
    $rule = $Slide.Shapes.AddShape(1, 48, 76, 864, 3)
    $rule.Fill.ForeColor.RGB = $(if ($Dark) { $Color.Teal } else { $Color.Mist })
    $rule.Fill.Solid()
    $rule.Line.Visible = 0
}

function Add-Footer {
    param($Slide, [int]$Number, [bool]$Dark = $false)
    $color = $(if ($Dark) { $Color.LightGray } else { $Color.Gray })
    Add-Text $Slide "CINESCOPE  |  BIG DATA 2026" 48 516 300 14 8 $color "Bahnschrift" $true | Out-Null
    Add-Text $Slide $Number.ToString("00") 860 512 50 18 9 $color "Bahnschrift" $true 2 | Out-Null
}

function Add-Notes($Slide, [string]$Notes) {
    try {
        $Slide.NotesPage.Shapes.Placeholders.Item(2).TextFrame.TextRange.Text = $Notes
    }
    catch {
        Write-Warning "Could not add notes to slide $($Slide.SlideIndex): $($_.Exception.Message)"
    }
}

function Add-ImageOrPlaceholder {
    param(
        $Slide,
        [string]$Path,
        [double]$X,
        [double]$Y,
        [double]$Width,
        [double]$Height,
        [string]$Placeholder
    )
    if (Test-Path $Path) {
        $picture = $Slide.Shapes.AddPicture($Path, 0, -1, 0, 0)
        $picture.LockAspectRatio = -1
        $scale = [Math]::Min($Width / $picture.Width, $Height / $picture.Height)
        $picture.Width = $picture.Width * $scale
        $picture.Height = $picture.Height * $scale
        $picture.Left = $X + (($Width - $picture.Width) / 2)
        $picture.Top = $Y + (($Height - $picture.Height) / 2)
        return
    }
    Add-Panel $Slide $X $Y $Width $Height $Color.White $Color.LightGray | Out-Null
    Add-Text $Slide "CORRECTED ARTIFACT PENDING" ($X + 24) ($Y + 35) ($Width - 48) 24 10 $Color.Coral "Bahnschrift" $true 2 | Out-Null
    Add-Text $Slide $Placeholder ($X + 24) ($Y + 75) ($Width - 48) 70 17 $Color.Gray "Aptos" $false 2 | Out-Null
}

function New-Slide([bool]$Dark = $false) {
    $slide = $Presentation.Slides.Add($Presentation.Slides.Count + 1, 12)
    $slide.FollowMasterBackground = 0
    $slide.Background.Fill.ForeColor.RGB = $(if ($Dark) { $Color.Ink } else { $Color.Paper })
    $slide.Background.Fill.Solid()
    return $slide
}

try {
    $PowerPoint = New-Object -ComObject PowerPoint.Application
    $Presentation = $PowerPoint.Presentations.Add()
    $Presentation.PageSetup.SlideWidth = 960
    $Presentation.PageSetup.SlideHeight = 540

    # 1. Title
    $slide = New-Slide $true
    $accent = $slide.Shapes.AddShape(1, 0, 0, 18, 540)
    $accent.Fill.ForeColor.RGB = $Color.Coral
    $accent.Fill.Solid()
    $accent.Line.Visible = 0
    Add-Text $slide "CINESCOPE" 62 126 800 82 48 $Color.White "Bahnschrift SemiBold" $true | Out-Null
    Add-Text $slide "Predicting audience reception and Oscar recognition at scale" 64 215 720 56 24 $Color.Mist "Aptos" | Out-Null
    Add-Text $slide "348,676 rated films  |  100.9M cast and crew relationships" 64 298 740 32 16 $Color.Gold "Bahnschrift" $true | Out-Null
    Add-Text $slide "Isha Dave  |  Kenji Funaki" 64 420 500 28 15 $Color.White "Aptos" | Out-Null
    Add-Text $slide "NYU Big Data  |  Summer 2026" 64 454 500 22 11 $Color.LightGray "Aptos" | Out-Null
    Add-Notes $slide "ISHA - 0:15. Film decisions are expensive and uncertain. CineScope asks how much useful signal we can extract before release, while also showing the distributed engineering required to build that evidence."

    # 2. Decision problem
    $slide = New-Slide
    Add-Title $slide "01" "One platform, two decision outcomes"
    Add-Text $slide "Decision context" 52 106 260 30 13 $Color.Teal "Bahnschrift" $true | Out-Null
    Add-Text $slide "Can pre-release information help a studio rank projects for deeper review?" 52 143 340 112 29 $Color.Ink "Bahnschrift SemiBold" $true | Out-Null
    Add-Panel $slide 430 114 218 278 $Color.White $Color.LightGray | Out-Null
    Add-Text $slide "AUDIENCE" 454 140 170 22 11 $Color.Coral "Bahnschrift" $true | Out-Null
    Add-Text $slide "Reception hit" 454 178 170 36 25 $Color.Ink "Bahnschrift SemiBold" $true | Out-Null
    Add-Text $slide "IMDb rating >= 7.0" 454 238 170 24 16 $Color.TealDark "Aptos" $true | Out-Null
    Add-Text $slide "and votes >= 1,000" 454 268 170 24 16 $Color.TealDark "Aptos" $true | Out-Null
    Add-Text $slide "Not box-office profit" 454 332 170 22 12 $Color.Gray "Aptos" | Out-Null
    Add-Panel $slide 672 114 218 278 $Color.TealDark $Color.TealDark | Out-Null
    Add-Text $slide "PRESTIGE" 696 140 170 22 11 $Color.Gold "Bahnschrift" $true | Out-Null
    Add-Text $slide "Oscar recognition" 696 178 170 62 25 $Color.White "Bahnschrift SemiBold" $true | Out-Null
    Add-Text $slide "Any mapped Academy" 696 250 170 24 16 $Color.Mist "Aptos" | Out-Null
    Add-Text $slide "Award nomination" 696 280 170 24 16 $Color.Mist "Aptos" | Out-Null
    Add-Text $slide "Rare outcome: about 1.1%" 696 332 170 22 12 $Color.LightGray "Aptos" | Out-Null
    Add-Text $slide "The output is a ranking and research aid, not an automatic greenlight." 52 438 838 34 18 $Color.Coral "Bahnschrift" $true 2 | Out-Null
    Add-Footer $slide 2
    Add-Notes $slide "ISHA - 0:45. We predict two narrow outcomes. A hit combines IMDb quality and minimum reach; it is not profitability. The awards outcome is any mapped Oscar nomination. In both cases the intended use is ranking projects for review, never automatic approval. Handoff: To show how we made this scalable and methodologically defensible, Kenji will walk through the pipeline and evaluation."

    # 3. Scale and architecture
    $slide = New-Slide
    Add-Title $slide "02" "The 100.9M-row relationship table is the workload"
    $stats = @(
        @("12.7M", "title records"),
        @("100.9M", "film-person edges"),
        @("13M", "people"),
        @("348,676", "rated movies")
    )
    for ($index = 0; $index -lt $stats.Count; $index++) {
        $left = 48 + ($index * 218)
        Add-Panel $slide $left 98 196 92 $Color.White $Color.LightGray | Out-Null
        Add-Text $slide $stats[$index][0] ($left + 16) 113 164 34 25 $(if ($index -eq 1) { $Color.Coral } else { $Color.TealDark }) "Bahnschrift SemiBold" $true 2 | Out-Null
        Add-Text $slide $stats[$index][1] ($left + 12) 153 172 20 11 $Color.Gray "Aptos" $false 2 | Out-Null
    }
    $stages = @("RAW TSV", "BRONZE", "SILVER", "ANALYTICS", "MODELS")
    $stageNotes = @("IMDb + Oscars", "typed + cleaned", "film features", "five findings", "LR vs GBT")
    for ($index = 0; $index -lt $stages.Count; $index++) {
        $left = 48 + ($index * 174)
        $fill = $(if ($index -eq 2) { $Color.TealDark } else { $Color.Mist })
        $text = $(if ($index -eq 2) { $Color.White } else { $Color.Ink })
        Add-Panel $slide $left 276 148 104 $fill $fill | Out-Null
        Add-Text $slide $stages[$index] ($left + 12) 296 124 24 12 $(if ($index -eq 2) { $Color.Gold } else { $Color.Teal }) "Bahnschrift" $true 2 | Out-Null
        Add-Text $slide $stageNotes[$index] ($left + 12) 332 124 28 13 $text "Aptos" $false 2 | Out-Null
        if ($index -lt 4) {
            Add-Text $slide ">" ($left + 150) 309 24 34 24 $Color.Coral "Bahnschrift" $true 2 | Out-Null
        }
    }
    Add-Text $slide "Local SSD for analytics and ML  |  HDFS + YARN for Dataproc batch ETL" 48 428 844 34 17 $Color.Ink "Bahnschrift" $true 2 | Out-Null
    Add-Footer $slide 3
    Add-Notes $slide "KENJI - 1:20. The final movie table is not the hard part. IMDb stores about 100.9 million principal relationships, so we must restrict, join, order by release year, and aggregate back to film grain. The pipeline uses explicit schemas, bronze and silver Parquet layers, then local analytics and ML. The same ETL jobs run locally or through YARN on Dataproc."

    # 4. Spark execution
    $slide = New-Slide $true
    Add-Title $slide "03" "Spark execution: less movement, reusable storage" $true
    Add-Text $slide "REFERENCE EXECUTION" 50 103 230 20 10 $Color.Gold "Bahnschrift" $true | Out-Null
    $execution = @(
        @("25.3 s", "baseline join"),
        @("121.4 s", "cast/crew features"),
        @("8.0 s", "Oscar aggregation"),
        @("~23 min", "Dataproc pipeline")
    )
    for ($index = 0; $index -lt $execution.Count; $index++) {
        $left = 48 + ($index * 218)
        Add-Panel $slide $left 135 196 98 $Color.TealDark $Color.Teal | Out-Null
        Add-Text $slide $execution[$index][0] ($left + 12) 153 172 32 24 $Color.White "Bahnschrift SemiBold" $true 2 | Out-Null
        Add-Text $slide $execution[$index][1] ($left + 12) 195 172 20 11 $Color.Mist "Aptos" $false 2 | Out-Null
    }
    Add-Panel $slide 48 276 522 176 (Get-Rgb 30 38 43) $Color.Teal | Out-Null
    Add-Text $slide "PHYSICAL PLAN EVIDENCE" 70 296 250 18 10 $Color.Coral "Bahnschrift" $true | Out-Null
    Add-Text $slide "BroadcastHashJoin [nconst], BuildRight`n  +- 100.9M principal relationships`n  +- BroadcastExchange small lookup" 70 328 462 88 17 $Color.White "Consolas" | Out-Null
    Add-Text $slide "The broadcast lookup demonstrates the optimization. It is isolated from predictive features." 606 288 286 116 23 $Color.White "Bahnschrift SemiBold" $true | Out-Null
    Add-Text $slide "Parquet + Snappy makes repeated column scans practical; no unmeasured speedup claim." 606 410 286 48 13 $Color.LightGray "Aptos" | Out-Null
    Add-Footer $slide 4 $true
    Add-Notes $slide "KENJI - 1:10. We reduced work early by restricting principals to rated movie IDs and materializing reusable Parquet. The physical plan confirms a BroadcastHashJoin for the small demonstration lookup. We do not claim a measured broadcast speedup because we did not run a controlled A/B benchmark. The local and Dataproc timings describe different environments."

    # 5. Validity correction
    $slide = New-Slide
    Add-Title $slide "04" "We corrected future-information leakage before final evaluation"
    $steps = @(
        @("1", "POINT-IN-TIME FEATURES", "Known cast/director status now uses only films released earlier."),
        @("2", "STRICT FEATURE CONTRACT", "Outcomes, Oscars, full-career fields, and vote snapshots are rejected."),
        @("3", "CHRONOLOGICAL HOLDOUT", "Train, validate, then test on later years with no random mixing."),
        @("4", "RARE-EVENT METRICS", "Select by PR AUC; report positive precision, recall, and F1.")
    )
    for ($index = 0; $index -lt $steps.Count; $index++) {
        $top = 104 + ($index * 94)
        Add-Text $slide $steps[$index][0] 56 $top 42 42 24 $Color.Coral "Bahnschrift SemiBold" $true 2 | Out-Null
        Add-Text $slide $steps[$index][1] 116 ($top - 1) 272 22 11 $Color.Teal "Bahnschrift" $true | Out-Null
        Add-Text $slide $steps[$index][2] 116 ($top + 27) 760 44 17 $Color.Ink "Aptos" | Out-Null
    }
    Add-Text $slide "July model metrics are superseded. Final values must come from feature-semantics version 2." 116 480 760 26 14 $Color.Coral "Bahnschrift" $true | Out-Null
    Add-Footer $slide 5
    Add-Notes $slide "KENJI - 1:20. Our audit found that the old known-person flags used lifetime ratings and votes, which let future success influence earlier films. We fixed the producer, versioned the corrected silver artifact, removed retrospective vote totals from models, and made notebooks reject stale data. We also replaced random splits and weighted F1 with chronological holdouts and positive-class metrics."

    # 6. Models
    $slide = New-Slide
    Add-Title $slide "05" "Model choice and threshold come from validation years"
    $hitAlgorithm = $(if ($null -ne $Hit) { $Hit.selected_candidate.algorithm } else { "PENDING V2 RERUN" })
    $awardsAlgorithm = $(if ($null -ne $Awards) { $Awards.selected_candidate.algorithm } else { "PENDING V2 RERUN" })
    $modelRows = @(
        @("AUDIENCE HIT", $hitAlgorithm, $(if ($null -ne $Hit) { Format-Metric $Hit.test_metrics.pr_auc } else { "PENDING" }), $(if ($null -ne $Hit) { Format-Metric $Hit.test_metrics.precision } else { "PENDING" }), $(if ($null -ne $Hit) { Format-Metric $Hit.test_metrics.recall } else { "PENDING" }), $(if ($null -ne $Hit) { Format-Metric $Hit.threshold_selection.selected.threshold } else { "PENDING" })),
        @("OSCAR NOMINATION", $awardsAlgorithm, $(if ($null -ne $Awards) { Format-Metric $Awards.test_metrics.pr_auc } else { "PENDING" }), $(if ($null -ne $Awards) { Format-Metric $Awards.test_metrics.precision } else { "PENDING" }), $(if ($null -ne $Awards) { Format-Metric $Awards.test_metrics.recall } else { "PENDING" }), $(if ($null -ne $Awards) { Format-Metric $Awards.threshold_selection.selected.threshold } else { "PENDING" }))
    )
    Add-Text $slide "OUTCOME" 50 108 170 18 10 $Color.Gray "Bahnschrift" $true | Out-Null
    Add-Text $slide "SELECTED MODEL" 230 108 210 18 10 $Color.Gray "Bahnschrift" $true | Out-Null
    Add-Text $slide "PR AUC" 500 108 90 18 10 $Color.Gray "Bahnschrift" $true 2 | Out-Null
    Add-Text $slide "PRECISION" 606 108 90 18 10 $Color.Gray "Bahnschrift" $true 2 | Out-Null
    Add-Text $slide "RECALL" 716 108 74 18 10 $Color.Gray "Bahnschrift" $true 2 | Out-Null
    Add-Text $slide "THRESHOLD" 808 108 96 18 10 $Color.Gray "Bahnschrift" $true 2 | Out-Null
    for ($index = 0; $index -lt $modelRows.Count; $index++) {
        $top = 138 + ($index * 78)
        $fill = $(if ($index -eq 0) { $Color.White } else { $Color.Mist })
        Add-Panel $slide 46 $top 868 62 $fill $fill | Out-Null
        Add-Text $slide $modelRows[$index][0] 62 ($top + 18) 150 24 12 $Color.TealDark "Bahnschrift" $true | Out-Null
        Add-Text $slide $modelRows[$index][1] 230 ($top + 16) 240 28 15 $Color.Ink "Aptos" $true | Out-Null
        Add-Text $slide $modelRows[$index][2] 500 ($top + 16) 90 28 15 $Color.Ink "Bahnschrift" $true 2 | Out-Null
        Add-Text $slide $modelRows[$index][3] 606 ($top + 16) 90 28 15 $Color.Ink "Bahnschrift" $true 2 | Out-Null
        Add-Text $slide $modelRows[$index][4] 716 ($top + 16) 74 28 15 $Color.Ink "Bahnschrift" $true 2 | Out-Null
        Add-Text $slide $modelRows[$index][5] 808 ($top + 16) 96 28 15 $Color.Ink "Bahnschrift" $true 2 | Out-Null
    }
    Add-ImageOrPlaceholder $slide $HitPrCurve 48 326 408 142 "hit_model_pr_curve.png after corrected rerun"
    Add-ImageOrPlaceholder $slide $AwardsPrCurve 504 326 408 142 "awards_model_pr_curve.png after corrected rerun"
    Add-Text $slide "PR AUC is primary because positives are rare; test years are evaluated once." 48 484 864 24 13 $Color.Coral "Bahnschrift" $true 2 | Out-Null
    Add-Footer $slide 6
    Add-Notes $slide "KENJI - 2:10. We compare class-weighted Logistic Regression with GBT. Candidate parameters are selected by validation PR AUC, then a validation threshold sweep chooses the positive-class F1 operating point. Only then do we evaluate later test years. Explain the final table and curves after the v2 rerun. Avoid accuracy and old weighted F1. Handoff: With the technical foundation and corrected evaluation in place, Isha will translate the results into film decisions."

    # 7. Findings: genre and runtime
    $slide = New-Slide
    Add-Title $slide "06" "Film context changes how ratings should be interpreted"
    Add-ImageOrPlaceholder $slide $GenreChart 46 104 430 306 "genre_decade_median_rating.png after corrected analytics rerun"
    Add-ImageOrPlaceholder $slide $RuntimeChart 494 104 420 306 "runtime_profile.png after corrected analytics rerun"
    Add-Text $slide "GENRE + ERA" 56 426 160 18 10 $Color.Teal "Bahnschrift" $true | Out-Null
    Add-Text $slide "Benchmark projects against comparable periods, not one timeless genre average." 56 452 382 44 16 $Color.Ink "Aptos" | Out-Null
    Add-Text $slide "RUNTIME PROFILE" 506 426 180 18 10 $Color.Coral "Bahnschrift" $true | Out-Null
    Add-Text $slide "Volume and rating differ by runtime, but the chart does not prove a causal sweet spot." 506 452 388 44 16 $Color.Ink "Aptos" | Out-Null
    Add-Footer $slide 7
    Add-Notes $slide "ISHA - 1:15. Genre ratings move across decades, so context matters. Runtime shows where films cluster and how ratings vary, but it is descriptive. We should not tell a studio that adding or removing minutes causes success."

    # 8. Findings: people and discovery
    $slide = New-Slide
    Add-Title $slide "07" "Creative history is useful signal, not a verdict"
    Add-ImageOrPlaceholder $slide $DirectorChart 46 104 430 286 "director_prior_vs_rating.png after corrected analytics rerun"
    $corr = $(if ($null -ne $Analytics) { Format-Metric $Analytics.director_prior_rating_corr } else { "PENDING V2" })
    $niche = $(if ($null -ne $Analytics) { [string]$Analytics.anomaly_count_rating_ge_8_bottom_decile_votes } else { "PENDING V2" })
    Add-Panel $slide 504 104 408 104 $Color.TealDark $Color.TealDark | Out-Null
    Add-Text $slide "DIRECTOR ASSOCIATION" 526 123 260 18 10 $Color.Gold "Bahnschrift" $true | Out-Null
    Add-Text $slide ("r = " + $corr) 526 152 350 38 28 $Color.White "Bahnschrift SemiBold" $true | Out-Null
    Add-Panel $slide 504 224 408 104 $Color.White $Color.LightGray | Out-Null
    Add-Text $slide "NICHE CANDIDATES" 526 243 260 18 10 $Color.Coral "Bahnschrift" $true | Out-Null
    Add-Text $slide $niche 526 272 350 38 28 $Color.Ink "Bahnschrift SemiBold" $true | Out-Null
    Add-ImageOrPlaceholder $slide $SignalLiftChart 504 344 408 112 "pre_release_signal_lift.png after corrected analytics rerun"
    Add-Text $slide "Association is not causation. Use these signals to prioritize diligence and discovery." 48 470 864 30 15 $Color.Coral "Bahnschrift" $true 2 | Out-Null
    Add-Footer $slide 8
    Add-Notes $slide "ISHA - 1:15. Director prior rating has a moderate association with the next film, but stronger directors may also receive better scripts, budgets, and distribution. High-rating, low-vote titles are niche discovery candidates, not proof of manipulation. The final lift chart uses prior experience rather than the circular vote-quartile analysis."

    # 9. Recommendations
    $slide = New-Slide
    Add-Title $slide "08" "Use CineScope to shortlist, then add human and economic context"
    Add-Text $slide "RECOMMEND" 50 104 160 20 10 $Color.Teal "Bahnschrift" $true | Out-Null
    Add-Bullets $slide @(
        "Rank projects for deeper creative and market review",
        "Choose thresholds to match false-positive and false-negative costs",
        "Benchmark genre and runtime within comparable eras",
        "Review niche candidates for catalog and acquisition opportunities"
    ) 50 137 418 17 66 $Color.Ink $Color.Teal
    Add-Text $slide "LIMIT" 526 104 160 20 10 $Color.Coral "Bahnschrift" $true | Out-Null
    Add-Bullets $slide @(
        "IMDb reception is not box-office profit or ROI",
        "Ratings are retrospective snapshots and users are self-selected",
        "Oscar labels depend on IMDb mapping and complete eligibility years",
        "Correlations do not establish causal creative decisions"
    ) 526 137 386 17 66 $Color.Ink $Color.Coral
    Add-Panel $slide 50 418 862 60 $Color.Ink $Color.Ink | Out-Null
    Add-Text $slide "Next-value data: budget, marketing, distribution, release footprint, and revenue" 74 436 814 24 16 $Color.White "Bahnschrift" $true 2 | Out-Null
    Add-Footer $slide 9
    Add-Notes $slide "ISHA - 1:30. The practical use is ranking and shortlisting. Different teams can choose different thresholds. But we must be explicit about what is missing: IMDb is not revenue, ratings are retrospective, and correlations are not causal. The next most valuable enrichment is economic and distribution data."

    # 10. Close
    $slide = New-Slide $true
    Add-Title $slide "09" "Three takeaways" $true
    $takeaways = @(
        @("01", "BIG DATA", "The engineering challenge is the 100.9M-row relationship graph and temporal aggregation."),
        @("02", "VALIDITY", "Point-in-time features and chronological evaluation matter more than preserving old headline metrics."),
        @("03", "DECISION USE", "CineScope is a transparent ranking aid, not an automatic greenlight system.")
    )
    for ($index = 0; $index -lt $takeaways.Count; $index++) {
        $top = 116 + ($index * 108)
        Add-Text $slide $takeaways[$index][0] 56 $top 48 28 15 $Color.Coral "Bahnschrift" $true | Out-Null
        Add-Text $slide $takeaways[$index][1] 126 $top 170 26 13 $Color.Gold "Bahnschrift" $true | Out-Null
        Add-Text $slide $takeaways[$index][2] 310 ($top - 4) 580 58 20 $Color.White "Aptos" | Out-Null
    }
    Add-Text $slide "Questions?" 56 462 836 42 30 $Color.Mist "Bahnschrift SemiBold" $true 2 | Out-Null
    Add-Footer $slide 10 $true
    Add-Notes $slide "ISHA - 1:00. Close on three points: the relationship graph makes this a real big-data workload; correcting validity is a strength, even if metrics decline; and the product is a transparent decision aid. Thank the audience and open three minutes of Q&A."

    # 11. Appendix: cohorts and labels
    $slide = New-Slide
    Add-Title $slide "A1" "Appendix: labels and chronological cohorts"
    Add-Panel $slide 48 106 414 134 $Color.White $Color.LightGray | Out-Null
    Add-Text $slide "AUDIENCE HIT" 70 126 200 20 11 $Color.Coral "Bahnschrift" $true | Out-Null
    Add-Text $slide "Rating >= 7.0 and votes >= 1,000" 70 158 360 28 20 $Color.Ink "Bahnschrift SemiBold" $true | Out-Null
    Add-Text $slide "Train 1960-2013  |  Validate 2014-2018  |  Test 2019-2023" 70 204 360 18 11 $Color.Gray "Aptos" | Out-Null
    Add-Panel $slide 498 106 414 134 $Color.TealDark $Color.TealDark | Out-Null
    Add-Text $slide "OSCAR NOMINATION" 520 126 230 20 11 $Color.Gold "Bahnschrift" $true | Out-Null
    Add-Text $slide "Any mapped nomination" 520 158 360 28 20 $Color.White "Bahnschrift SemiBold" $true | Out-Null
    Add-Text $slide "Train 1960-2007  |  Validate 2008-2013  |  Test 2014-2019" 520 204 360 18 11 $Color.LightGray "Aptos" | Out-Null
    Add-Text $slide "Hit-label sensitivity matrix" 48 284 360 24 17 $Color.Ink "Bahnschrift SemiBold" $true | Out-Null
    Add-ImageOrPlaceholder $slide $SensitivityChart 48 326 864 142 "Sensitivity values are exported in analytics_metrics.json"
    Add-Footer $slide 11
    Add-Notes $slide "Q&A. Explain why the hit label combines quality and reach, and why the Oscar test period stops in 2019. Point out that thresholds are documented and sensitivity is exported."

    # 12. Appendix: full model metrics
    $slide = New-Slide
    Add-Title $slide "A2" "Appendix: model metrics and confusion matrices"
    Add-ImageOrPlaceholder $slide $HitConfusion 48 104 414 322 "Corrected hit confusion matrix"
    Add-ImageOrPlaceholder $slide $AwardsConfusion 498 104 414 322 "Corrected Oscar confusion matrix"
    Add-Text $slide "Headline metrics exclude frequency-weighted F1; all hard metrics use validation-selected thresholds." 48 458 864 28 14 $Color.Coral "Bahnschrift" $true 2 | Out-Null
    Add-Footer $slide 12
    Add-Notes $slide "Q&A. Use this slide for precision, recall, confusion counts, and why accuracy is misleading for 4% and 1.1% positive rates."

    # 13. Appendix: physical plan
    $slide = New-Slide $true
    Add-Title $slide "A3" "Appendix: broadcast join evidence" $true
    Add-Panel $slide 48 106 864 300 (Get-Rgb 30 38 43) $Color.Teal | Out-Null
    Add-Text $slide "BroadcastHashJoin [nconst], [nconst], LeftOuter, BuildRight`n  :- SortMergeJoin [... principal history ...]`n  +- BroadcastExchange HashedRelationBroadcastMode(...)`n       +- small descriptive known-person lookup" 76 144 808 156 19 $Color.White "Consolas" | Out-Null
    Add-Text $slide "Why it matters" 76 326 180 22 12 $Color.Gold "Bahnschrift" $true | Out-Null
    Add-Text $slide "Broadcast the small side once instead of shuffling the 100.9M-row relationship side. The lookup is plan evidence only and is not a model predictor." 76 358 808 54 17 $Color.Mist "Aptos" | Out-Null
    Add-Footer $slide 13 $true
    Add-Notes $slide "Q&A. Explain build-right broadcast and the separation between optimization evidence and point-in-time predictive features. Do not claim a measured speedup."

    # 14. Appendix: limitations
    $slide = New-Slide
    Add-Title $slide "A4" "Appendix: data coverage and remaining risks"
    Add-Bullets $slide @(
        "348,676 final rows are rated movies, not every IMDb title",
        "Oscar rows without usable IMDb IDs cannot become positive labels",
        "Older films have more time to accumulate ratings and votes",
        "Prior ratings are retrospective proxies; historical vote totals are excluded",
        "One chronological holdout design does not measure every future regime",
        "No budget, marketing, distribution, revenue, or profitability fields"
    ) 58 112 840 18 58 $Color.Ink $Color.Coral
    Add-Footer $slide 14
    Add-Notes $slide "Q&A. Use this slide for data bias, film-population questions, timestamp limitations, and why the system does not predict box office."

    if (Test-Path $OutputPath) {
        Remove-Item $OutputPath -Force
    }
    $Presentation.SaveAs($OutputPath, 24)
    Write-Output "Wrote $OutputPath"
    Write-Output "Slides: $($Presentation.Slides.Count)"
    Write-Output "Version 2 metrics loaded: analytics=$($null -ne $Analytics), hit=$($null -ne $Hit), awards=$($null -ne $Awards)"
}
finally {
    if ($null -ne $Presentation) {
        try {
            $Presentation.Close()
        }
        catch {
            Write-Warning "PowerPoint rejected Close during cleanup: $($_.Exception.Message)"
        }
        [Runtime.InteropServices.Marshal]::ReleaseComObject($Presentation) | Out-Null
    }
    if ($null -ne $PowerPoint) {
        try {
            $PowerPoint.Quit()
        }
        catch {
            Write-Warning "PowerPoint rejected Quit during cleanup: $($_.Exception.Message)"
        }
        [Runtime.InteropServices.Marshal]::ReleaseComObject($PowerPoint) | Out-Null
    }
    [GC]::Collect()
    [GC]::WaitForPendingFinalizers()
}