set terminal pdfcairo enhanced
set datafile separator "\t"
set style data histogram
set style histogram clustered
set termoption font ',16'

# Line style for axes
set style line 80 lt 0 lc rgb "#808080"

# Set the border using the linestyle 80 that we defined
# 3 = 1 + 2 (1 = plot the bottom line and 2 = plot the left line)
# back means the border should be behind anything else drawn
set border 3 back ls 80

# Line style for grid
# Define a new linestyle (81)
# linetype = 0 (dashed line)
# linecolor = gray
# lw = lineweight, make it half as wide as the axes lines
set style line 81 lt 0 lc rgb "#808080" lw 0.5

# Draw the grid lines for both the major and minor tics
set grid xtics
set grid ytics
set grid mxtics
set grid mytics

# Put the grid behind anything drawn and use the linestyle 81
set grid back ls 81
set termoption dash

set style line 1 lw 1
set style line 2 lw 1
set style line 3 lw 1
set style line 4 lw 1
set style increment user


set key default
set key box
set key width 2 height 1
set key inside right top
set key box width 2
set key samplen 2
set key spacing 1

# Sizeof OX i OY (numbers)
set tics font ", 18"

# Legend font
set key font "Helvetica, 16"

# Title font (above picture)
set title font "Helvetica, 24"

#  Fonts OY
set ylabel font "Helvetica, 28"

# Fonts OX
set xlabel font "Helvetica, 28"

# OPTIMAL K

set key default
set key box
set key width 2 height 1
set key inside left top
set key box width 2
set key samplen 2
set key spacing 1

set output "./experimentPlots/phd/fa/ex0_several_k_samsungK9F1G_tpcc_warehouse.pdf"
set title ""
set ylabel "Czas [s]"
plot for [col=2:5] './experimentResults/phd/fa/real/ex0_several_k_samsungK9F1G_tpcc_warehouse.txt'  using col:xticlabels(1) lt -1 fs pattern (col == 3 ? 7 : col == 5 ? 1 : col) title columnheader


set key default
set key box
set key width 2 height 1
set key inside right top
set key box width 2
set key samplen 2
set key spacing 1

# BASIC REAL TIME
# set output "./experimentPlots/phd/fa/ex1_basic_samsungK9F1G_tpcc_neworder.pdf"
# set title ""
# set ylabel "Czas [s]"
# plot for [col=2:4] './experimentResults/phd/fa/real/ex1_basic_samsungK9F1G_tpcc_neworder.txt'  using col:xticlabels(1) lt -1 fs pattern (col == 3 ? 7 : col) title columnheader

# set output "./experimentPlots/phd/fa/ex1_basic_micronMT29AAA_tpcc_neworder.pdf"
# set title ""
# set ylabel "Czas [s]"
# plot for [col=2:4] './experimentResults/phd/fa/real/ex1_basic_micronMT29AAA_tpcc_neworder.txt'  using col:xticlabels(1) lt -1 fs pattern (col == 3 ? 7 : col) title columnheader

# set output "./experimentPlots/phd/fa/ex1_basic_micronMT29_WC1_tpcc_neworder.pdf"
# set title ""
# set ylabel "Czas [s]"
# plot for [col=2:4] './experimentResults/phd/fa/real/ex1_basic_micronMT29_WC1_tpcc_neworder.txt'  using col:xticlabels(1) lt -1 fs pattern (col == 3 ? 7 : col) title columnheader



# set output "./experimentPlots/phd/fa/ex1_basic_samsungK9F1G_tpcc_warehouse.pdf"
# set title ""
# set ylabel "Czas [s]"
# plot for [col=2:4] './experimentResults/phd/fa/real/ex1_basic_samsungK9F1G_tpcc_warehouse.txt'  using col:xticlabels(1) lt -1 fs pattern (col == 3 ? 7 : col) title columnheader

# set output "./experimentPlots/phd/fa/ex1_basic_micronMT29AAA_tpcc_warehouse.pdf"
# set title ""
# set ylabel "Czas [s]"
# plot for [col=2:4] './experimentResults/phd/fa/real/ex1_basic_micronMT29AAA_tpcc_warehouse.txt'  using col:xticlabels(1) lt -1 fs pattern (col == 3 ? 7 : col) title columnheader

# set output "./experimentPlots/phd/fa/ex1_basic_micronMT29_WC1_tpcc_warehouse.pdf"
# set title ""
# set ylabel "Czas [s]"
# plot for [col=2:4] './experimentResults/phd/fa/real/ex1_basic_micronMT29_WC1_tpcc_warehouse.txt'  using col:xticlabels(1) lt -1 fs pattern (col == 3 ? 7 : col) title columnheader


#  Fonts OY
set ylabel font "Helvetica, 16"

# BASIC NORMALIZED TIME
# set output "./experimentPlots/phd/fa/ex1_basic_samsungK9F1G_tpcc_neworder_normalized.pdf"
# set title ""
# set ylabel "Czas znormalizowany do FA-Tree"
# plot for [col=2:4] './experimentResults/phd/fa/real/ex1_basic_samsungK9F1G_tpcc_neworder_normalized.txt'  using col:xticlabels(1) lt -1 fs pattern (col == 3 ? 7 : col) title columnheader

# set output "./experimentPlots/phd/fa/ex1_basic_micronMT29AAA_tpcc_neworder_normalized.pdf"
# set title ""
# set ylabel "Czas znormalizowany do FA-Tree"
# plot for [col=2:4] './experimentResults/phd/fa/real/ex1_basic_micronMT29AAA_tpcc_neworder_normalized.txt'  using col:xticlabels(1) lt -1 fs pattern (col == 3 ? 7 : col) title columnheader

# set output "./experimentPlots/phd/fa/ex1_basic_micronMT29_WC1_tpcc_neworder_normalized.pdf"
# set title ""
# set ylabel "Czas znormalizowany do FA-Tree"
# plot for [col=2:4] './experimentResults/phd/fa/real/ex1_basic_micronMT29_WC1_tpcc_neworder_normalized.txt'  using col:xticlabels(1) lt -1 fs pattern (col == 3 ? 7 : col) title columnheader



set output "./experimentPlots/phd/fa/ex1_basic_samsungK9F1G_tpcc_warehouse_normalized.pdf"
set title ""
set ylabel "Czas znormalizowany do FA-Tree"
plot for [col=2:4] './experimentResults/phd/fa/real/ex1_basic_samsungK9F1G_tpcc_warehouse_normalized.txt'  using col:xticlabels(1) lt -1 fs pattern (col == 3 ? 7 : col) title columnheader

set output "./experimentPlots/phd/fa/ex1_basic_micronMT29AAA_tpcc_warehouse_normalized.pdf"
set title ""
set ylabel "Czas znormalizowany do FA-Tree"
plot for [col=2:4] './experimentResults/phd/fa/real/ex1_basic_micronMT29AAA_tpcc_warehouse_normalized.txt'  using col:xticlabels(1) lt -1 fs pattern (col == 3 ? 7 : col) title columnheader

set output "./experimentPlots/phd/fa/ex1_basic_micronMT29_WC1_tpcc_warehouse_normalized.pdf"
set title ""
set ylabel "Czas znormalizowany do FA-Tree"
plot for [col=2:4] './experimentResults/phd/fa/real/ex1_basic_micronMT29_WC1_tpcc_warehouse_normalized.txt'  using col:xticlabels(1) lt -1 fs pattern (col == 3 ? 7 : col) title columnheader


# EX2 WAREHOUSE normalized
set output "./experimentPlots/phd/fa/ex2_extendend_samsungK9F1G_tpcc_warehouse_normalized.pdf"
set title ""
set ylabel "Czas znormalizowany do FA-Tree"
plot for [col=2:4] './experimentResults/phd/fa/real/ex2_extendend_samsungK9F1G_tpcc_warehouse_normalized.txt'  using col:xticlabels(1) lt -1 fs pattern (col == 3 ? 7 : col) title columnheader

set output "./experimentPlots/phd/fa/ex2_extendend_micronMT29AAA_tpcc_warehouse_normalized.pdf"
set title ""
set ylabel "Czas znormalizowany do FA-Tree"
plot for [col=2:4] './experimentResults/phd/fa/real/ex2_extendend_micronMT29AAA_tpcc_warehouse_normalized.txt'  using col:xticlabels(1) lt -1 fs pattern (col == 3 ? 7 : col) title columnheader

set output "./experimentPlots/phd/fa/ex2_extendend_micronMT29_WC1_tpcc_warehouse_normalized.pdf"
set title ""
set ylabel "Czas znormalizowany do FA-Tree"
plot for [col=2:4] './experimentResults/phd/fa/real/ex2_extendend_micronMT29_WC1_tpcc_warehouse_normalized.txt'  using col:xticlabels(1) lt -1 fs pattern (col == 3 ? 7 : col) title columnheader


# EX2 MICRON WC1 NEW ORDER
set output "./experimentPlots/phd/fa/ex2_extendend_micronMT29_WC1_tpcc_neworder_normalized.pdf"
set title ""
set ylabel "Czas znormalizowany do FA-Tree"
plot for [col=2:4] './experimentResults/phd/fa/real/ex2_extendend_micronMT29_WC1_tpcc_neworder_normalized.txt'  using col:xticlabels(1) lt -1 fs pattern (col == 3 ? 7 : col) title columnheader


# EX2 MICRON WC1 CLIENT
