package compress;

public class UniformSampler extends Compressor {


    vector<int> Uniform(int uniform){
        vector<int> simplified;
        int originalIndex = 0;
        while (true){
            if (originalIndex > points.size())
                break;
            else
                simplified.push_back(originalIndex);
            originalIndex = originalIndex + uniform;
        }
        simplified.push_back(points.size() - 1);
        return simplified;
    }



}
