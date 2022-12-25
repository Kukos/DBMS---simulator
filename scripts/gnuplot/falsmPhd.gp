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
set key box width -4
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
set yrange[0:90]
set output "./experimentPlots/phd/falsm/ex0_20rsearches_samsung_warehouse.pdf"
set title ""
set ylabel "CZAS [s]"
set xlabel "Parametr T"
plot for [col=2:5] './experimentResults/phd/falsm/real/ex0_20rsearches_samsung_warehouse.txt' using 1:col title columnheader

set yrange[0:*]
set output "./experimentPlots/phd/falsm/ex0_40rsearches_samsung_warehouse.pdf"
set title ""
set ylabel "CZAS [s]"
set xlabel "Parametr T"
plot for [col=2:5] './experimentResults/phd/falsm/real/ex0_40rsearches_samsung_warehouse.txt' using 1:col title columnheader

set yrange[0:*]
set output "./experimentPlots/phd/falsm/ex0_100rsearches_samsung_warehouse.pdf"
set title ""
set ylabel "CZAS [s]"
set xlabel "Parametr T"
plot for [col=2:5] './experimentResults/phd/falsm/real/ex0_100rsearches_samsung_warehouse.txt' using 1:col title columnheader

set yrange[0:*]
set output "./experimentPlots/phd/falsm/ex0_250rsearches_samsung_warehouse.pdf"
set title ""
set ylabel "CZAS [s]"
set xlabel "Parametr T"
plot for [col=2:5] './experimentResults/phd/falsm/real/ex0_250rsearches_samsung_warehouse.txt' using 1:col title columnheader


set yrange[0:30]
set output "./experimentPlots/phd/falsm/ex0_40rsearches_samsung_neworder.pdf"
set title ""
set ylabel "CZAS [s]"
set xlabel "Parametr T"
plot for [col=2:5] './experimentResults/phd/falsm/real/ex0_40rsearches_samsung_neworder.txt' using 1:col title columnheader

set yrange[0:50]
set output "./experimentPlots/phd/falsm/ex0_100rsearches_samsung_neworder.pdf"
set title ""
set ylabel "CZAS [s]"
set xlabel "Parametr T"
plot for [col=2:5] './experimentResults/phd/falsm/real/ex0_100rsearches_samsung_neworder.txt' using 1:col title columnheader

set yrange[0:1000]
set output "./experimentPlots/phd/falsm/ex0_40rsearches_samsung_customer.pdf"
set title ""
set ylabel "CZAS [s]"
set xlabel "Parametr T"
plot for [col=2:5] './experimentResults/phd/falsm/real/ex0_40rsearches_samsung_customer.txt' using 1:col title columnheader

set yrange[0:1500]
set output "./experimentPlots/phd/falsm/ex0_100rsearches_samsung_customer.pdf"
set title ""
set ylabel "CZAS [s]"
set xlabel "Parametr T"
plot for [col=2:5] './experimentResults/phd/falsm/real/ex0_100rsearches_samsung_customer.txt' using 1:col title columnheader



set yrange[0:150]
set output "./experimentPlots/phd/falsm/ex0_40rsearches_toshiba_warehouse.pdf"
set title ""
set ylabel "CZAS [s]"
set xlabel "Parametr T"
plot for [col=2:5] './experimentResults/phd/falsm/real/ex0_40rsearches_toshiba_warehouse.txt' using 1:col title columnheader

set yrange[0:300]
set output "./experimentPlots/phd/falsm/ex0_100rsearches_toshiba_warehouse.pdf"
set title ""
set ylabel "CZAS [s]"
set xlabel "Parametr T"
plot for [col=2:5] './experimentResults/phd/falsm/real/ex0_100rsearches_toshiba_warehouse.txt' using 1:col title columnheader


set yrange[0:50]
set output "./experimentPlots/phd/falsm/ex0_40rsearches_intel_warehouse.pdf"
set title ""
set ylabel "CZAS [s]"
set xlabel "Parametr T"
plot for [col=2:5] './experimentResults/phd/falsm/real/ex0_40rsearches_intel_warehouse.txt' using 1:col title columnheader

set yrange[0:*]
set output "./experimentPlots/phd/falsm/ex0_100rsearches_intel_warehouse.pdf"
set title ""
set ylabel "CZAS [s]"
set xlabel "Parametr T"
plot for [col=2:5] './experimentResults/phd/falsm/real/ex0_100rsearches_intel_warehouse.txt' using 1:col title columnheader


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
set ylabel font "Helvetica, 16"

# Fonts OX
set xlabel font "Helvetica, 28"


set key default
set key box
set key width 2 height 1
set key inside left top
set key box width 2
set key samplen 2
set key spacing 1

# TIME

set yrange[0:*]
set output "./experimentPlots/phd/falsm/ex4_extendend_samsung_tpcc_warehouse_normalized.pdf"
set title ""
set ylabel "Czas znormalizowany do FALSM-Tree"
plot for [col=2:3] './experimentResults/phd/falsm/real/ex4_extendend_samsung_tpcc_warehouse_normalized.txt'  using col:xticlabels(1) lt -1 fs pattern (col == 3 ? 7 : col) title columnheader

set yrange[0:*]
set output "./experimentPlots/phd/falsm/ex4_extendend_samsung_tpcc_neworder_normalized.pdf"
set title ""
set ylabel "Czas znormalizowany do FALSM-Tree"
plot for [col=2:3] './experimentResults/phd/falsm/real/ex4_extendend_samsung_tpcc_neworder_normalized.txt'  using col:xticlabels(1) lt -1 fs pattern (col == 3 ? 7 : col) title columnheader

set yrange[0:*]
set output "./experimentPlots/phd/falsm/ex4_extendend_samsung_tpcc_customer_normalized.pdf"
set title ""
set ylabel "Czas znormalizowany do FALSM-Tree"
plot for [col=2:3] './experimentResults/phd/falsm/real/ex4_extendend_samsung_tpcc_customer_normalized.txt'  using col:xticlabels(1) lt -1 fs pattern (col == 3 ? 7 : col) title columnheader

set yrange[0:*]
set output "./experimentPlots/phd/falsm/ex4_extendend_intel_tpcc_warehouse_normalized.pdf"
set title ""
set ylabel "Czas znormalizowany do FALSM-Tree"
plot for [col=2:3] './experimentResults/phd/falsm/real/ex4_extendend_intel_tpcc_warehouse_normalized.txt'  using col:xticlabels(1) lt -1 fs pattern (col == 3 ? 7 : col) title columnheader

set yrange[0:*]
set output "./experimentPlots/phd/falsm/ex4_extendend_toshiba_tpcc_warehouse_normalized.pdf"
set title ""
set ylabel "Czas znormalizowany do FALSM-Tree"
plot for [col=2:3] './experimentResults/phd/falsm/real/ex4_extendend_toshiba_tpcc_warehouse_normalized.txt'  using col:xticlabels(1) lt -1 fs pattern (col == 3 ? 7 : col) title columnheader
