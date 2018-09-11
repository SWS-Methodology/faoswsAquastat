apply_interpolation_to_selected_elements <- function(x){
  if (ncol(x) == 3) {
    name <- names(x)[3]
    fixed <- x[, 1:2]
    imputable <- x[, 3]
    imputed <- as.data.frame(imputeTS::na.interpolation(imputable))
    names(imputed) <- name
    x <- cbind(fixed, imputed)
  } else if (ncol(x) == 2) { 
    x <- x[, 1:2]
  } else {
    fixed <- x[, 1:2]
    imputable <- x[, 3:ncol(x)]
    imputed <- imputeTS::na.interpolation(imputable)
    x <- cbind(fixed, imputed)
  }})