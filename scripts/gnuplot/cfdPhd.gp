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
set style line 1 lt 1 lc rgb "#A00000" lw 2 pt 19 ps 1.5
set style line 2 lt 1 lc rgb "#5060D0" lw 2 pt 8 ps 1.5
set style line 3 lt 1 lc rgb "#D0D000" lw 2 pt 6 ps 1.5
set style line 4 lt 2 lc rgb "black" lw 2 dt 4
#set style line 5 lt 1 lc rgb "#7cff40" lw 2 pt 1 ps 1.5

set style increment user
set style data linespoints

set key default
set key box
set key ins vert left top Left maxrows 2
set key box width -1
set key samplen 2

# Sizeof OX i OY (numbers)
set tics font ", 18"

# Legend font
set key font "Helvetica, 16"

# Title font (above picture)
set title font "Helvetica, 24"

#  Fonts OY
set ylabel font "Helvetica, 28"

# Fonts osi OX
set xlabel font "Helvetica, 28"


#set xtics 5
set yrange[0:750]
set output "./experimentPlots/phd/cfd/ex1_basic_intel_tpcc_customer_ZP_{odczyt}.pdf"
set title ""
set ylabel "CZAS [s]"
set xlabel "Liczba kolejnych zwracanych kolumn"
plot for [col=2:4] './experimentResults/phd/cfd/real/ex1_basic_intel_tpcc_customer_ZP_{odczyt}.txt' using 1:col title columnheader

set yrange[0:800]
set output "./experimentPlots/phd/cfd/ex1_basic_intel_tpcc_warehouse_ZP_{odczyt}.pdf"
set title ""
set ylabel "CZAS [s]"
set xlabel "Liczba kolejnych zwracanych kolumn"
plot for [col=2:4] './experimentResults/phd/cfd/real/ex1_basic_intel_tpcc_warehouse_ZP_{odczyt}.txt' using 1:col title columnheader

set yrange[0:1550]
set output "./experimentPlots/phd/cfd/ex1_basic_samsung_tpcc_customer_ZP_{odczyt}.pdf"
set title ""
set ylabel "CZAS [s]"
set xlabel "Liczba kolejnych zwracanych kolumn"
plot for [col=2:4] './experimentResults/phd/cfd/real/ex1_basic_samsung_tpcc_customer_ZP_{odczyt}.txt' using 1:col title columnheader

set yrange[0:2400]
set output "./experimentPlots/phd/cfd/ex1_basic_samsung_tpcc_warehouse_ZP_{odczyt}.pdf"
set title ""
set ylabel "CZAS [s]"
set xlabel "Liczba kolejnych zwracanych kolumn"
plot for [col=2:4] './experimentResults/phd/cfd/real/ex1_basic_samsung_tpcc_warehouse_ZP_{odczyt}.txt' using 1:col title columnheader

set yrange[0:1450]
set output "./experimentPlots/phd/cfd/ex1_basic_toshiba_tpcc_customer_ZP_{odczyt}.pdf"
set title ""
set ylabel "CZAS [s]"
set xlabel "Liczba kolejnych zwracanych kolumn"
plot for [col=2:4] './experimentResults/phd/cfd/real/ex1_basic_toshiba_tpcc_customer_ZP_{odczyt}.txt' using 1:col title columnheader

set yrange[0:2400]
set output "./experimentPlots/phd/cfd/ex1_basic_toshiba_tpcc_warehouse_ZP_{odczyt}.pdf"
set title ""
set ylabel "CZAS [s]"
set xlabel "Liczba kolejnych zwracanych kolumn"
plot for [col=2:4] './experimentResults/phd/cfd/real/ex1_basic_toshiba_tpcc_warehouse_ZP_{odczyt}.txt' using 1:col title columnheader


set yrange[0:1200]
set output "./experimentPlots/phd/cfd/ex2_basic_intel_tpcc_customer_ZP_{odczyt}.pdf"
set title ""
set ylabel "CZAS [s]"
set xlabel "Liczba kolejnych zwracanych kolumn"
plot for [col=2:4] './experimentResults/phd/cfd/real/ex2_basic_intel_tpcc_customer_ZP_{odczyt}.txt' using 1:col title columnheader


set yrange[0:2800]
set output "./experimentPlots/phd/cfd/ex2_basic_intel_tpcc_customer_5_ZP_{odczyt}.pdf"
set title ""
set ylabel "CZAS [s]"
set xlabel "Liczba kolejnych zwracanych kolumn"
plot for [col=2:4] './experimentResults/phd/cfd/real/ex2_basic_intel_tpcc_customer_5_ZP_{odczyt}.txt' using 1:col title columnheader


set yrange[0:5500]
set output "./experimentPlots/phd/cfd/ex2_basic_intel_tpcc_customer_10_ZP_{odczyt}.pdf"
set title ""
set ylabel "CZAS [s]"
set xlabel "Liczba kolejnych zwracanych kolumn"
plot for [col=2:4] './experimentResults/phd/cfd/real/ex2_basic_intel_tpcc_customer_10_ZP_{odczyt}.txt' using 1:col title columnheader


set yrange[0:50]
set output "./experimentPlots/phd/cfd/ex3_extendend_intel_tpcc_customer_1_ZR_{A}.pdf"
set title ""
set ylabel "CZAS [s]"
set xlabel "Liczba kolejnych zwracanych kolumn"
plot for [col=2:3] './experimentResults/phd/cfd/real/ex3_extendend_intel_tpcc_customer_1_ZR_{A}.txt' using 1:col title columnheader

set yrange[0:5]
set output "./experimentPlots/phd/cfd/ex3_extendend_intel_tpcc_customer_1_ZR_{B}.pdf"
set title ""
set ylabel "CZAS [s]"
set xlabel "Liczba kolejnych zwracanych kolumn"
plot for [col=2:3] './experimentResults/phd/cfd/real/ex3_extendend_intel_tpcc_customer_1_ZR_{B}.txt' using 1:col title columnheader

set yrange[0:1500]
set output "./experimentPlots/phd/cfd/ex3_extendend_intel_tpcc_customer_1_ZR_{C}.pdf"
set title ""
set ylabel "CZAS [s]"
set xlabel "Liczba kolejnych zwracanych kolumn"
plot for [col=2:3] './experimentResults/phd/cfd/real/ex3_extendend_intel_tpcc_customer_1_ZR_{C}.txt' using 1:col title columnheader


set yrange[0:120]
set output "./experimentPlots/phd/cfd/ex3_extendend_intel_tpcc_customer_1_ZR_{D}.pdf"
set title ""
set ylabel "CZAS [s]"
set xlabel "Liczba kolejnych zwracanych kolumn"
plot for [col=2:3] './experimentResults/phd/cfd/real/ex3_extendend_intel_tpcc_customer_1_ZR_{D}.txt' using 1:col title columnheader

set yrange[0:7]
set output "./experimentPlots/phd/cfd/ex3_extendend_intel_tpcc_warehouse_1_ZR_{A}.pdf"
set title ""
set ylabel "CZAS [s]"
set xlabel "Liczba kolejnych zwracanych kolumn"
plot for [col=2:3] './experimentResults/phd/cfd/real/ex3_extendend_intel_tpcc_warehouse_1_ZR_{A}.txt' using 1:col title columnheader

set yrange[0:250]
set output "./experimentPlots/phd/cfd/ex3_extendend_intel_tpcc_warehouse_1_ZR_{C}.pdf"
set title ""
set ylabel "CZAS [s]"
set xlabel "Liczba kolejnych zwracanych kolumn"
plot for [col=2:3] './experimentResults/phd/cfd/real/ex3_extendend_intel_tpcc_warehouse_1_ZR_{C}.txt' using 1:col title columnheader


set yrange[0:250]
set output "./experimentPlots/phd/cfd/ex3_extendend_intel_tpcc_customer_5_ZR_{A}.pdf"
set title ""
set ylabel "CZAS [s]"
set xlabel "Liczba kolejnych zwracanych kolumn"
plot for [col=2:3] './experimentResults/phd/cfd/real/ex3_extendend_intel_tpcc_customer_5_ZR_{A}.txt' using 1:col title columnheader


set yrange[0:1800]
set output "./experimentPlots/phd/cfd/ex3_extendend_intel_tpcc_customer_5_ZR_{C}.pdf"
set title ""
set ylabel "CZAS [s]"
set xlabel "Liczba kolejnych zwracanych kolumn"
plot for [col=2:3] './experimentResults/phd/cfd/real/ex3_extendend_intel_tpcc_customer_5_ZR_{C}.txt' using 1:col title columnheader
