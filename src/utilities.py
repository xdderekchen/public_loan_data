import time
from datetime import datetime
import pandas as pd

# Show runtime duration since tsart
def showtime(tstart):
    te = time.time()
    return f"{int((te - tstart) * 1000)} ms"

# Can be used as the function decorator
def decorator_time(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = f"{int((te - ts) * 1000)} ms"
        else:
            print('%r  %2.2f ms' % \
                  (method.__name__, (te - ts) * 1000)
                  )
        return result
    return timed


def compute_amortization(principals, monthly_rates, terms,  start_period = 0, end_period = None):
    """
    Compute amortization of loans

    Parameters
    ----------
    principals : scalar or array_like of shape(M, )
        pricipals of loans
    monthly_rates: scalar or array_like if  principals is a scalar
                   or
                   array_like or matrix_like if principals is an array_like
        For FRM, one Rate for each loan
        For ARM, one full time series for each loan
    
    terms:  scalar or array_like of shape(M, )
    loan terms, type should match that of principals

    Returns
    -------
    out : ndarray (M, N)

    """
    num_loans = 1
    if isinstance(principals, pd.Series):
       principals = principals.values
    elif np.ndim(principals) == 0:
       principals = [principals]
       
    #assume principals is np.array
    num_loans = len(principals)

    if isinstance(monthly_rates, pd.Series):
       monthly_rates = monthly_rates.values
    elif np.ndim(principals) == 0:
       monthly_rates = [monthly_rates]

    if isinstance(terms, pd.Series):
       terms = terms.values
    elif np.ndim(terms) == 0:
       terms = [terms]
    
    num_month = terms.max()
    mini_term = terms.min()
    if end_period is not None:
      num_month = min(num_month, max(1, (end_period - start_period)))
    
    upb_matrix = np.zeros((num_month, num_loans))
    the_payment = principals * (monthly_rates / (1 - (1 + monthly_rates) ** (-terms)))
    
    current_upb = principals
    if start_period > 0:
       current_upb = principals
       for t in range(1, start_period+1):
          pp_payment = the_payment - current_upb * monthly_rates
          current_upb = current_upb - pp_payment
    
    upb_matrix[0, :] = current_upb
      
    i = 1
    for t in range(start_period+1, start_period +num_month ):
       isbeyondTerm = np.greater(t , terms)
       pp_payment = the_payment - upb_matrix[i-1, :] * monthly_rates
       upb_matrix[i, :] = (upb_matrix[i-1, :] - pp_payment) 
       if t >= mini_term:
          iswithinTerm = np.greater( terms, t).astype(int)
          isbeyondTerm = np.greater(t , terms).astype(int)
          upb_matrix[i, :] = upb_matrix[i, :] * np.array(iswithinTerm) + (-999) * isbeyondTerm
       i = i +  1
    return upb_matrix.T
