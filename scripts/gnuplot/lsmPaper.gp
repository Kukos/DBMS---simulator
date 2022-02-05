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


# I played a while with 2y axes
#set xtics 5
#set yrange[0:90]
#set y2range[0:90]
#set output "./experimentPlots/lsmPaper/ex0_20rsearches.pdf"
#set multiplot
#set title ""
#set ylabel "TIME [s]"
#set xlabel "Parameter T"
#plot './experimentResults/lsmPaper/paper/ex0_20rsearches.txt' using 1:2 axes x1y1 title columnheader ls 1, \
# './experimentResults/lsmPaper/paper/ex0_20rsearches.txt' using 1:3 axes x1y2 title columnheader ls 2, \
# './experimentResults/lsmPaper/paper/ex0_20rsearches.txt' using 1:4 axes x1y2 title columnheader ls 3, \
# './experimentResults/lsmPaper/paper/ex0_20rsearches.txt' using 1:5 axes x1y2 title columnheader ls 4
#set nomultiplot

#set xtics 5
set yrange[0:90]
set output "./experimentPlots/lsmPaper/ex0_20rsearches.pdf"
set title ""
set ylabel "TIME [s]"
set xlabel "Parameter T"
plot for [col=2:5] './experimentResults/lsmPaper/paper/ex0_20rsearches.txt' using 1:col title columnheader

#set xtics 5
set yrange[0:*]
set output "./experimentPlots/lsmPaper/ex0_40rsearches.pdf"
set title ""
set ylabel "TIME [s]"
set xlabel "Parameter T"
plot for [col=2:5] './experimentResults/lsmPaper/paper/ex0_40rsearches.txt' using 1:col title columnheader

#set xtics 5
set yrange[0:*]
set output "./experimentPlots/lsmPaper/ex0_100rsearches.pdf"
set title ""
set ylabel "TIME [s]"
set xlabel "Parameter T"
plot for [col=2:5] './experimentResults/lsmPaper/paper/ex0_100rsearches.txt' using 1:col title columnheader

#set xtics 5
set yrange[0:*]
set output "./experimentPlots/lsmPaper/ex0_250rsearches.pdf"
set title ""
set ylabel "TIME [s]"
set xlabel "Parameter T"
plot for [col=2:5] './experimentResults/lsmPaper/paper/ex0_250rsearches.txt' using 1:col title columnheader

set key box width 0
set key samplen 2

set yrange[0:60]
set output "./experimentPlots/lsmPaper/ex1_0rsearches.pdf"
set title ""
set ylabel "TIME [s]"
set xlabel "Batch size [10^3 entries]"
plot for [col=2:5] './experimentResults/lsmPaper/paper/ex1_0rsearches.txt' using 1:col title columnheader

set yrange[0:100]
set output "./experimentPlots/lsmPaper/ex2_20rsearches_T4.pdf"
set title ""
set ylabel "TIME [s]"
set xlabel "SSTable size [MB]"
plot for [col=2:5] './experimentResults/lsmPaper/paper/ex2_20rsearches_T4.txt' using 1:col title columnheader

set yrange[0:100]
set output "./experimentPlots/lsmPaper/ex3_20rsearches_T4.pdf"
set title ""
set ylabel "TIME [s]"
set xlabel "MemTable size [MB]"
plot for [col=2:5] './experimentResults/lsmPaper/paper/ex3_20rsearches_T4.txt' using 1:col title columnheader

set xtics 10
set yrange[0:*]
set output "./experimentPlots/lsmPaper/ex5_20rsearches_T4.pdf"
set title ""
set ylabel "TIME [s]"
set xlabel "Database size [10^6 entries]"
plot for [col=2:5] './experimentResults/lsmPaper/paper/ex5_20rsearches_T4.txt' using 1:col title columnheader
