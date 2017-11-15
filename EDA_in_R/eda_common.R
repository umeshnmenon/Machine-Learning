require(ggplot2)
#' Plots the bar chart of data available for each of the variables compared to the sample size
#'
#' Plots the bar chart of data available for each of the variables compared to the number of rows in input data
#'
#' @param input_df A data.frame. Required. A data.frame using which the plot is created.
#' @return Returns a ggplot object
#' @examples
#' plot.bar.missing_counts(input_df)
#' @export plot.bar.missing_counts
plot.bar.missing_counts <- function(input_df){

  missing_counts <-  as.data.frame(colSums(!is.na(input_df))/dim(input_df)[1]*100)
  names(missing_counts) <- c("present_data_perc")
  missing_counts$variable_names <- rownames(missing_counts)
  #missing_counts <- missing_counts[order(missing_counts$present_data_perc),]
  plt <- ggplot(missing_counts,aes(x=variable_names,y=present_data_perc)) +
         coord_flip() + geom_bar(stat="identity") + theme(axis.text.y = element_text(size=rel(0.8))) +
         scale_y_continuous("percentage data present")
  return(plt)
}

#' Plots the density graph of variables
#'
#' Plots the density graph of variables. 
#'
#' @param input_df A data.frame. Required. A data.frame using which the plot is created.
#' @param selected_vars A character vector. A list of variable names to be plotted. If not set, density plot of
#' all the variables will be used plotted.
#' @return Returns a list of ggplot objects
#' @examples
#' plot.density.variables(input_df)
#' #Selecting variables that have above 25% data availability for plotting
#' missing_counts <- as.data.frame(colSums(!is.na(input_df))/dim(input_df)[1]*100)
#' names(missing_counts) <- c("present_data_per")
#' missing_counts$variable_names <- rownames(missing_counts)
#' sel_cols <- names(input_df)[missing_counts$present_data_perc >= 25]
#' #Filter in only numeric variables
#' sel_cols <- sel_cols[grep("amount_spent", sel_cols)]
#' plot.density.variables(input_df, sel_cols)
#' @export plot.density.variables
plot.density.variables <- function(input_df, selected_vars = NULL, yvar=NULL, facet=FALSE){
  if (is.null(selected_vars)){
    selected_vars <- names(input_df)
  }
  plotlst <- list()

  for(sel_var in selected_vars){
    if (facet){
      plt <- ggplot(input_df, aes_string(x=sel_var, fill=yvar)) +
        geom_density(alpha=0.5) +
        xlab(sel_var) + facet_wrap(as.formula(paste("~", yvar, sep=""))) + guides(fill=F)
    }else{
      plt <- ggplot(input_df, aes_string(x=sel_var, fill=yvar)) +
        geom_density(alpha=0.5) +
        xlab(sel_var) 
    }
    
    plotlst <- list(plotlst, list(plt))
    
  }
  #print(plotlst)
  invisible(capture.output(print(plotlst)))
  #return(plotlst)
}

#' Plots the density graph of variables using histogram
#'
#' Plots the density graph of variables using histogram. 
#'
#' @param input_df A data.frame. Required. A data.frame using which the plot is created.
#' @param selected_vars A character vector. A list of variable names to be plotted. If not set, density plot of
#' all the variables will be used plotted.
#' @return Returns a list of ggplot objects
#' @examples
#' plot.histgram.variables(input_df)
#' #Selecting variables that have above 25% data availability for plotting
#' missing_counts <- as.data.frame(colSums(!is.na(input_df))/dim(input_df)[1]*100)
#' names(missing_counts) <- c("present_data_per")
#' missing_counts$variable_names <- rownames(missing_counts)
#' sel_cols <- names(input_df)[missing_counts$present_data_perc >= 25]
#' #Filter in only numeric variables
#' sel_cols <- sel_cols[grep("amount_spent", sel_cols)]
#' plot.histgram.variables(input_df, sel_cols)
#' @export plot.histgram.variables
plot.histgram.variables <- function(input_df, selected_vars = NULL, yvar=NULL, facet=FALSE){
  if (is.null(selected_vars)){
    selected_vars <- names(input_df)
  }
  plotlst <- list()
  
  for(sel_var in selected_vars){
    plt <- ggplot(input_df, aes_string(x=sel_var, fill=yvar)) +
      geom_histogram(binwidth=2.0, alpha=0.5, position="identity") +
      xlab(sel_var)
    if (facet){
      plt <- plt + facet_wrap(as.formula(paste("~", yvar, sep=""))) + guides(fill=F)
    }
    
    plotlst <- list(plotlst, list(plt))
  }
  invisible(capture.output(print(plotlst)))
  #return(plotlst)
}

#' Plots the boxplot and violin plot of various variables for a given team
#'
#' Plots the boxplot and violin plot of various variables for a given team.
#'
#' @param input_df A data.frame. Required. A data.frame using which the plot is created.
#' @param xvar A character. Name of the team for which the variables to be plotted
#' @param selected_vars A character vector. A list of variable names to be plotted. If not set, density plot of
#' all the variables will be used plotted.
#' @return Returns a list of ggplot objects
#' @examples
#' plot.boxplot.variables(input_df, "category_1")
#' #Selecting variables that have above 25% data availability for plotting
#' missing_counts <-  as.data.frame(colSums(!is.na(input_df))/dim(input_df)[1]*100)
#' names(missing_counts) <- c("present_data_per")
#' missing_counts$variable_names <- rownames(missing_counts)
#' sel_cols <- names(input_df)[missing_counts$present_data_perc >= 25]
#' #Filter in only numeric variables
#' sel_cols <- sel_cols[grep("amount_spent", sel_cols)]
#' plot.boxplot.variables(input_df, "category_1", sel_cols)
#' @export plot.boxplot.variables
plot.boxplot.variables <- function(input_df, selected_vars = NULL, xvar= NULL, plotviolin=FALSE){
  if (is.null(selected_vars)){
    selected_vars <- names(input_df)
  }
  plotlst <- list()
  for(sel_col in selected_vars){
    #selected_col_val <- input_df[,sel_col]
    if (is.null(xvar)){
      plt <- ggplot(input_df, aes_string(x="factor(0)", y= sel_col))  
    }else{
      plt <- ggplot(input_df, aes_string(x = xvar , y= sel_col,
                                  fill = xvar))
    }
    
    if (plotviolin){
      plt <- plt +
        geom_boxplot(alpha=0.5) + ylab(sel_col) + geom_violin(alpha = 0.5)   
    }else{
      plt <- plt +
        geom_boxplot() + ylab(sel_col)
    }

    plotlst <- list(plotlst, list(plt))
  }
  #print(plotlst)
  invisible(capture.output(print(plotlst)))
  #return(plotlst)
}


#' Plots the bar chart for a given variable
#'
#' Plots the bar chart for a variable. The bars show the count of occurences.
#'
#' @param input_df A data.frame. Required. A data.frame using which the plot is created.
#' @param xvar A character.  Name of the variable for which the frequency to be plotted
#' @param selected_vars A character vector. A list of variable names to be plotted. If not set, density plot of
#' all the variables will be used plotted.
#' @return Returns a list of ggplot objects
#' @examples
#' plot.barplot.variables(input_df, "usertype")
#' @export plot.barplot.variables
plot.barplot.variables <- function(input_df, selected_vars = NULL){
  if (is.null(selected_vars)){
    selected_vars <- names(input_df)
  }
  plotlst <- list()
  for(sel_col in selected_vars){
    plt <- ggplot(input_df, aes_string(x=paste("factor(",sel_col,")", sep=""))) + 
      geom_bar(stat="count") + xlab(sel_col)
    plotlst <- list(plotlst, list(plt))
  }
  #print(plotlst)
  invisible(capture.output(print(plotlst)))
  #return(plotlst)
}
