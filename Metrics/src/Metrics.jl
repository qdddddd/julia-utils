module Metrics
export r2

using Statistics

function r2(y_true, y_pred)
    ssr = sum((y_true .- y_pred) .^ 2)
    tot = sum((y_true .- mean(y_true)) .^ 2)
    return 1 - ssr / tot
end

end
