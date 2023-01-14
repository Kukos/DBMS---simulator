set terminal pdfcairo enhanced
set datafile separator "\t"
set termoption font ',16'

# Line style for axes
# Define a line style (we're calling it 80) and set
# lt = linetype to 0 (dashed line)
# lc = linecolor to a gray defined by that number
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
# Create some linestyles for our data
# pt = point type (triangles, circles, squares, etc.)
# ps = point size
# set style line 1 lt -1 lw 3 lc rgb '#990042' ps 1 pt 6 pi 1
# set style line 2 lt -1 lw 3 lc rgb '#31f120' ps 1 pt 12 pi 1
# set style line 3 lt -1 lw 3 lc rgb '#0044a5' ps 1 pt 9 pi 1
# set style line 4 lt -1 lw 3 lc rgb '#888888' ps 1 pt 7 pi 1
set style line 1 lt 1 lc rgb "#A00000" lw 1 pt 0
set style line 2 lt 1 lc rgb "#5060D0" lw 1 pt 0
set style line 3 lt 1 lc rgb "#D0D000" lw 1 pt 0
set style line 4 lt 1 lc rgb "#7cff40" lw 1 pt 0
set style line 5 lt 1 lc rgb "black" lw 1 pt 0

set style increment user
set style data linespoints

set key default
set key box
set key ins vert left top Left maxrows 3
set key box width -4
set key samplen 2

# Sizeof OX i OY (numbers)
set tics font ", 18"

# Legend font
set key font "Helvetica, 14"

# Title font (above picture)
set title font "Helvetica, 24"

#  Fonts OY
set ylabel font "Helvetica, 28"

# Fonts osi OX
set xlabel font "Helvetica, 28"


set xtics 100
set yrange[0:1.8]
set output "./experimentPlots/phd/pam/ex1_basic_pcmdefault_tpcc_warehouse_1_query_time.pdf"
set title ""
set ylabel "CZAS [s]"
set xlabel "Numer kwerendy"
plot for [col=2:6] './experimentResults/phd/pam/real/ex1_basic_pcmdefault_tpcc_warehouse_1_query_time.txt' using 1:col title columnheader

set xtics 20
set yrange[0:8]
set output "./experimentPlots/phd/pam/ex1_basic_pcmdefault_tpcc_warehouse_5_query_time.pdf"
set title ""
set ylabel "CZAS [s]"
set xlabel "Numer kwerendy"
plot for [col=2:6] './experimentResults/phd/pam/real/ex1_basic_pcmdefault_tpcc_warehouse_5_query_time.txt' using 1:col title columnheader


set xtics 100
set yrange[0:10]
set output "./experimentPlots/phd/pam/ex1_basic_pcmdefault_tpcc_customer_1_query_time.pdf"
set title ""
set ylabel "CZAS [s]"
set xlabel "Numer kwerendy"
plot for [col=2:6] './experimentResults/phd/pam/real/ex1_basic_pcmdefault_tpcc_customer_1_query_time.txt' using 1:col title columnheader

set xtics 20
set yrange[0:45]
set output "./experimentPlots/phd/pam/ex1_basic_pcmdefault_tpcc_customer_5_query_time.pdf"
set title ""
set ylabel "CZAS [s]"
set xlabel "Numer kwerendy"
plot for [col=2:6] './experimentResults/phd/pam/real/ex1_basic_pcmdefault_tpcc_customer_5_query_time.txt' using 1:col title columnheader

reset


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


# Sizeof OX i OY (numbers)
set tics font ", 18"

# Legend font
set key font "Helvetica, 14"

# Title font (above picture)
set title font "Helvetica, 24"

#  Fonts OY
set ylabel font "Helvetica, 28"

# Fonts OX
set xlabel font "Helvetica, 28"


set key box
set key width 1 height 1
set key ins vert left top Left maxrows 2
set key box width -4
set key samplen 1
#set key spacing 1

set yrange[0:1300]
set output "./experimentPlots/phd/pam/ex2_extendend_pcmdefault_tpcc_warehouse_1.pdf"
set title ""
set ylabel "Czas [s]"
plot for [col=2:4] './experimentResults/phd/pam/real/ex2_extendend_pcmdefault_tpcc_warehouse_1.txt'  using col:xticlabels(1) lt -1 fs pattern (col == 3 ? 7 : col == 5 ? 1 : col) title columnheader


set yrange[0:1600]
set output "./experimentPlots/phd/pam/ex2_extendend_pcmdefault_tpcc_warehouse_5.pdf"
set title ""
set ylabel "Czas [s]"
plot for [col=2:4] './experimentResults/phd/pam/real/ex2_extendend_pcmdefault_tpcc_warehouse_5.txt'  using col:xticlabels(1) lt -1 fs pattern (col == 3 ? 7 : col == 5 ? 1 : col) title columnheader

set yrange[0:7200]
set output "./experimentPlots/phd/pam/ex2_extendend_pcmdefault_tpcc_customer_1.pdf"
set title ""
set ylabel "Czas [s]"
plot for [col=2:4] './experimentResults/phd/pam/real/ex2_extendend_pcmdefault_tpcc_customer_1.txt'  using col:xticlabels(1) lt -1 fs pattern (col == 3 ? 7 : col == 5 ? 1 : col) title columnheader

set yrange[0:9000]
set output "./experimentPlots/phd/pam/ex2_extendend_pcmdefault_tpcc_customer_5.pdf"
set title ""
set ylabel "Czas [s]"
plot for [col=2:4] './experimentResults/phd/pam/real/ex2_extendend_pcmdefault_tpcc_customer_5.txt'  using col:xticlabels(1) lt -1 fs pattern (col == 3 ? 7 : col == 5 ? 1 : col) title columnheader
